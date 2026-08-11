package server

import (
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/a-shan/mysql-pitr/internal/server/agent"
	"github.com/a-shan/mysql-pitr/internal/server/audit"
	"github.com/a-shan/mysql-pitr/internal/server/auth"
	"github.com/a-shan/mysql-pitr/internal/server/org"
	"github.com/a-shan/mysql-pitr/internal/server/pitr"
	"github.com/a-shan/mysql-pitr/internal/server/store"
)

// newWebRouter builds the REST + SPA router from pre-initialised handlers.
func newWebRouter(
	agentStore agent.AgentStore,
	orgStore org.OrgStore,
	userStore auth.UserStore,
	authHandler *auth.Handler,
	orgHandler *org.Handler,
	agentHandler *agent.Handler,
	pitrHandler *pitr.Handler,
	auditHandler *audit.Handler,
) *chi.Mux {
	r := chi.NewRouter()

	// Global middleware.
	r.Use(chiMiddleware.Logger)
	r.Use(chiMiddleware.Recoverer)
	r.Use(chiMiddleware.RequestID)
	r.Use(chiMiddleware.RealIP)

	// ---- Public routes ----
	r.Route("/api/auth", func(r chi.Router) {
		r.Post("/register", authHandler.Register)
		r.Post("/login", authHandler.Login)
		r.Post("/refresh", authHandler.Refresh)
	})

	// ---- Protected routes (JWT required) ----
	r.Group(func(r chi.Router) {
		r.Use(authHandler.AuthMiddleware)

		// Organisation endpoints.
		r.Route("/api/orgs", func(r chi.Router) {
			r.Get("/", orgHandler.List)
			r.Post("/", orgHandler.Create)

			r.Route("/{id}", func(r chi.Router) {
				r.Post("/invite", orgHandler.Invite)
				r.Post("/accept", orgHandler.AcceptInvite)
				r.Get("/members", orgHandler.ListMembers)
			})
		})

		// Agent endpoints.
		r.Route("/api/agents", func(r chi.Router) {
			r.Post("/register", agentHandler.Register)
			r.Get("/", agentHandler.List)

			r.Route("/{id}", func(r chi.Router) {
				r.Post("/approve", agentHandler.Approve)
				r.Post("/reject", agentHandler.Reject)
				r.Get("/", agentHandler.Get)
				r.Get("/archive", agentHandler.ArchiveStatus)
			})
		})

		// PITR workflow endpoints.
		r.Route("/api/pitr", func(r chi.Router) {
			r.Post("/start", pitrHandler.Start)
			r.Get("/", pitrHandler.List)

			r.Route("/{id}", func(r chi.Router) {
				r.Get("/status", pitrHandler.Status)
				r.Get("/transactions", pitrHandler.Transactions)
				r.Post("/select", pitrHandler.Select)
				r.Post("/execute", pitrHandler.Execute)
				r.Post("/pause", pitrHandler.Pause)
				r.Post("/resume", pitrHandler.Resume)
				r.Post("/cancel", pitrHandler.Cancel)
				r.Get("/events", pitrHandler.Events)
			})
		})

		// Audit log endpoints.
		r.Route("/api/audit", func(r chi.Router) {
			r.Get("/", auditHandler.Query)
			r.Get("/export", auditHandler.Export)
		})
	})

	// ---- Frontend SPA ----
	// Serve the embedded placeholder frontend (embed_stub/) for all non-API
	// routes: real stub files resolve through the embed filesystem, and
	// everything else falls back to index.html for client-side routing.
	// Phase 4 replaces the stub with the real SvelteKit build (web/).
	fileServer := http.FileServer(http.FS(stubFS))
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		// Unknown /api routes are 404, not SPA fallback: a typo'd API path must
		// not be answered with the front-end index page. Known API routes are
		// matched by the routes above and never reach this catch-all.
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name != "" && name != "." {
			if f, err := stubFS.Open(name); err == nil {
				info, statErr := f.Stat()
				_ = f.Close()
				if statErr == nil && !info.IsDir() {
					fileServer.ServeHTTP(w, r)
					return
				}
			}
		}
		// SPA fallback: serve the placeholder index page.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(placeholderIndex)
	})

	return r
}

// NewRouter creates and configures a chi router with all API routes mounted.
// It is a lightweight dev/test helper: every store is in-memory (the pitr
// store uses a throwaway, freshly migrated SQLite database) and the PITR flow
// has no agent command channel. Production deployments should use New() from
// server.go for the persistent SQLite database, CA and mTLS agent listener.
func NewRouter() *chi.Mux {
	jwtSecret := []byte(os.Getenv("JWT_SECRET"))
	if len(jwtSecret) == 0 {
		jwtSecret = []byte("change-me-in-production")
	}

	userStore := auth.NewInMemoryUserStore()
	orgStore := org.NewInMemoryOrgStore()
	agentStore := agent.NewInMemoryAgentStore()
	db, err := store.Open(":memory:")
	if err != nil {
		panic("pitr: open sqlite: " + err.Error())
	}
	if err := store.Migrate(db); err != nil {
		panic("pitr: migrate sqlite: " + err.Error())
	}
	pitrStore := pitr.NewSQLiteOperationStore(db)
	auditStore := audit.NewInMemoryAuditStore()

	authHandler := auth.NewHandler(userStore, jwtSecret)
	orgHandler := org.NewHandler(orgStore, userStore, jwtSecret)
	agentHandler := agent.NewHandler(agentStore, orgStore, jwtSecret, nil)
	pitrHandler := pitr.NewHandler(pitrStore, agentStore, orgStore, auditStore, pitr.NewEventBus(), nil, jwtSecret)
	auditHandler := audit.NewHandler(auditStore, orgStore, jwtSecret)

	return newWebRouter(agentStore, orgStore, userStore, authHandler, orgHandler, agentHandler, pitrHandler, auditHandler)
}
