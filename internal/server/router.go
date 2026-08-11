package server

import (
	"net/http"
	"os"
	"path/filepath"

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
			})
		})

		// PITR workflow endpoints.
		r.Route("/api/pitr", func(r chi.Router) {
			r.Post("/start", pitrHandler.Start)
			r.Get("/", pitrHandler.List)

			r.Route("/{id}", func(r chi.Router) {
				r.Get("/status", pitrHandler.Status)
				r.Post("/cancel", pitrHandler.Cancel)
				r.Get("/preview", pitrHandler.Preview)
				r.Get("/progress", pitrHandler.Progress)
				r.Post("/execute", pitrHandler.Execute)
			})
		})

		// Audit log endpoints.
		r.Route("/api/audit", func(r chi.Router) {
			r.Get("/", auditHandler.Query)
			r.Get("/export", auditHandler.Export)
		})
	})

	// ---- Frontend SPA ----
	// Serve the built Vite frontend for all non-API routes.
	webDir := os.Getenv("WEB_DIR")
	if webDir == "" {
		webDir = "/usr/share/mysql-pitr/web"
	}
	fs := http.FileServer(http.Dir(webDir))
	r.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		path := filepath.Clean(r.URL.Path)
		if path != "/" {
			if _, err := os.Stat(filepath.Join(webDir, path)); err == nil {
				fs.ServeHTTP(w, r)
				return
			}
		}
		// SPA fallback: serve index.html for client-side routing.
		http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
	})

	return r
}

// NewRouter creates and configures a chi router with all API routes mounted.
// It wires a fresh in-memory CA + hub; production deployments should use
// New() from server.go for a persistent CA and the mTLS agent listener.
func NewRouter() *chi.Mux {
	jwtSecret := []byte(os.Getenv("JWT_SECRET"))
	if len(jwtSecret) == 0 {
		jwtSecret = []byte("change-me-in-production")
	}

	userStore := auth.NewInMemoryUserStore()
	orgStore := org.NewInMemoryOrgStore()
	agentStore := agent.NewInMemoryAgentStore()
	// TODO(Task 7): wire the shared platform SQLite database; the pitr store
	// currently uses a throwaway in-memory database so each NewRouter gets an
	// isolated, migrated schema.
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
	agentHandler := agent.NewHandler(agentStore, orgStore, jwtSecret)
	pitrHandler := pitr.NewHandler(pitrStore, agentStore, orgStore, auditStore, jwtSecret, nil)
	auditHandler := audit.NewHandler(auditStore, orgStore, jwtSecret)

	return newWebRouter(agentStore, orgStore, userStore, authHandler, orgHandler, agentHandler, pitrHandler, auditHandler)
}
