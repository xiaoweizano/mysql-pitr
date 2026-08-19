package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	gorilla "github.com/gorilla/websocket"

	"github.com/a-shan/mysql-pitr/internal/server/agent"
	"github.com/a-shan/mysql-pitr/internal/server/audit"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/server/pitr"
	"github.com/a-shan/mysql-pitr/internal/server/store"
	"github.com/a-shan/mysql-pitr/internal/ws"
	"github.com/a-shan/mysql-pitr/internal/ws/ca"
	"github.com/a-shan/mysql-pitr/internal/ws/hub"
)

// Server bundles the two listeners of the mysql-pitr-server: the public web
// API (Web) and the mTLS agent endpoint (Agent).
type Server struct {
	// Web serves the REST API and SPA on LISTEN_ADDR.
	Web http.Handler
	// Agent serves the mTLS /ws/agent WebSocket endpoint on AGENT_LISTEN_ADDR.
	Agent http.Handler
	// TLSConfig is the mTLS configuration for the agent listener.
	TLSConfig *tls.Config
	// DataDir is where CA material (root + revocations) is persisted.
	DataDir string
	// Hub is the live agent hub, exposed for observability and tests.
	Hub *hub.Hub

	closeFn func()
}

// New initialises the shared SQLite database, all stores, the internal CA,
// the agent hub, and the two routers. CA state (ca.json) and the platform
// database (app.db) persist under AGENT_DATA_DIR (default "./data").
func New() (*Server, error) {
	return newServer("", nil)
}

// newServer is New with an explicit data dir and an optional agent command
// channel. commander is the AgentCommander behind the PITR flow: nil selects
// the live agent hub (production); tests inject a fake so the HTTP flow can
// be smoke-tested without a connected WebSocket agent.
func newServer(dataDir string, commander pitr.AgentCommander) (*Server, error) {
	if dataDir == "" {
		dataDir = os.Getenv("AGENT_DATA_DIR")
		if dataDir == "" {
			dataDir = "data"
		}
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("server: create data dir: %w", err)
	}

	// Internal CA for agent mTLS: root cert and revocations persist across
	// restarts in a single JSON file.
	rootCA := ca.NewCA(ca.NewFileStorage(filepath.Join(dataDir, "ca.json")))
	if _, err := rootCA.GenerateRoot(); err != nil {
		return nil, err
	}

	agentHub := hub.NewHub("")
	agentHub.SetCSRHandler(rootCA)

	// ---- stores: one shared SQLite database for every domain ----
	db, err := store.Open(filepath.Join(dataDir, "app.db"))
	if err != nil {
		return nil, fmt.Errorf("server: open sqlite: %w", err)
	}
	if err := store.Migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("server: migrate sqlite: %w", err)
	}
	userStore := auth.NewSQLiteUserStore(db)
	orgStore := org.NewSQLiteOrgStore(db)
	agentStore := agent.NewSQLiteAgentStore(db)
	pitrStore := pitr.NewSQLiteOperationStore(db)
	auditStore := audit.NewSQLiteAuditStore(db)

	// ---- handlers ----
	authHandler := auth.NewHandler(userStore, jwtSecret())
	reassignFn := func(fromOrgID, toOrgID string) error {
		agents, err := agentStore.ListByOrg(fromOrgID)
		if err != nil {
			return err
		}
		for _, a := range agents {
			a.OrgID = toOrgID
			if err := agentStore.Update(a); err != nil {
				return err
			}
		}
		return nil
	}
	orgHandler := org.NewHandler(orgStore, userStore, jwtSecret(), reassignFn)
	agentHandler := agent.NewHandler(agentStore, orgStore, jwtSecret(), agentHub)
	pitrBus := pitr.NewEventBus()
	if commander == nil {
		commander = agentHub
	}
	pitrHandler := pitr.NewHandler(pitrStore, agentStore, orgStore, auditStore, pitrBus, commander, jwtSecret())
	auditHandler := audit.NewHandler(auditStore, orgStore, jwtSecret())

	// Route agent→server stream_event envelopes (scan tx/SQL, execution
	// progress, operation completion) into the PITR handler, which fans them
	// out to SSE subscribers.
	agentHub.SetStreamEventHandler(func(agentID string, cmd ws.Command) {
		pitrHandler.HandleStreamEvent(agentID, cmd)
	})

	// Keep the agents API's status field in sync with the hub. Unknown agents
	// (e.g. after a server restart cleared the in-memory store) are registered
	// automatically so they show up in the UI and are selectable in PITR.
	agentHub.SetLifecycleHooks(hub.LifecycleHooks{
		OnConnect: func(agentID string) {
			if rec, err := agentStore.Get(agentID); err == nil {
				rec.Status = "online"
				rec.LastSeen = time.Now()

				// Async: fetch MySQL version via CmdPreflight.
				// Non-blocking: version stays empty on failure.
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					resp, err := agentHub.SendToAgent(ctx, agentID, ws.Command{
						Cmd:    agentID,
						Type:   ws.CmdPreflight,
						Params: map[string]interface{}{},
					})
					if err != nil || resp.Status == ws.StatusError {
						return
					}
					b, _ := json.Marshal(resp.Result)
					var pf struct {
						Preflight *struct {
							Version string `json:"version"`
						} `json:"preflight"`
					}
					if err := json.Unmarshal(b, &pf); err != nil || pf.Preflight == nil || pf.Preflight.Version == "" {
						return
					}
					if rec, err := agentStore.Get(agentID); err == nil {
						rec.MySQLVersion = pf.Preflight.Version
						_ = agentStore.Update(rec)
					}
				}()

				_ = agentStore.Update(rec)
				return
			}

			// Presenting a CA-signed mTLS certificate proves registration, so
			// trust it and auto-register the agent under the first org.
			orgs, _ := orgStore.ListAll()
			var orgID string
			if len(orgs) > 0 {
				orgID = orgs[0].ID
			}
			rec := &agent.AgentRecord{
				ID:        agentID,
				OrgID:     orgID,
				Hostname:  agentID,
				Status:    "online",
				LastSeen:  time.Now(),
				CreatedAt: time.Now(),
				Approved:  true,
			}
			if err := agentStore.Create(rec); err == nil {
				log.Printf("server: auto-registered connected agent %s", agentID)
			}
		},
		OnDisconnect: func(agentID string) {
			if rec, err := agentStore.Get(agentID); err == nil {
				rec.Status = "offline"
				rec.LastSeen = time.Now()
				_ = agentStore.Update(rec)
			}
			// The agent is gone: any operation it was scanning or executing
			// cannot make progress, so block it (terminal) with an audit.
			pitrHandler.HandleAgentDisconnect(agentID)
		},
	})

	// ---- routers ----
	webRouter := newWebRouter(agentStore, orgStore, userStore, authHandler, orgHandler, agentHandler, pitrHandler, auditHandler)
	agentMux := http.NewServeMux()
	agentMux.HandleFunc("/ws/agent", func(w http.ResponseWriter, r *http.Request) {
		upgrader := gorilla.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("hub: upgrade failed: %v", err)
			return
		}
		agentHub.HandleConnection(conn, r)
	})

	// mTLS configuration: clients must present a certificate signed by the
	// internal CA; the server presents a CA-signed server certificate so
	// agents can verify the endpoint.
	hosts := strings.Split(os.Getenv("AGENT_CERT_HOSTS"), ",")
	if len(hosts) == 0 || hosts[0] == "" {
		hosts = []string{"localhost", "server"}
	}
	serverCert, err := rootCA.SignServerCert(hosts)
	if err != nil {
		return nil, err
	}
	tlsCfg := rootCA.ServerTLSConfig()
	tlsCfg.Certificates = []tls.Certificate{*serverCert}

	srv := &Server{
		Web:       webRouter,
		Agent:     agentMux,
		TLSConfig: tlsCfg,
		DataDir:   dataDir,
		Hub:       agentHub,
	}
	srv.closeFn = func() {
		_ = agentHub.Close()
		_ = db.Close()
	}

	// Startup reconcile: every agent is offline at boot, so scanning /
	// executing operations left over from a previous run are blocked (terminal
	// + audit). Failures are logged, never fatal — the server must start even
	// when historical operations are in a bad state.
	if err := pitrHandler.ReconcileOnStartup(); err != nil {
		log.Printf("server: pitr startup reconcile: %v", err)
	}

	// Mark all agents offline: nothing is connected at boot. Agents that
	// reconnect are set online by the hub lifecycle hook; stale records
	// (whose containers no longer exist) stay offline instead of showing a
	// stale "online" state.
	if err := agentStore.MarkAllOffline(); err != nil {
		log.Printf("server: mark agents offline: %v", err)
	}

	return srv, nil
}

// Close shuts down the agent hub and the shared SQLite database.
func (s *Server) Close() {
	if s.closeFn != nil {
		s.closeFn()
	}
}

func jwtSecret() []byte {
	secret := os.Getenv("JWT_SECRET")
	if len(secret) == 0 {
		log.Println("WARNING: JWT_SECRET not set — using insecure default. Set JWT_SECRET for production.")
		return []byte("change-me-in-production")
	}
	return []byte(secret)
}