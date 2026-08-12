package server

import (
	"embed"
	"io/fs"
)

// embedStub bundles the placeholder frontend (embed_stub/) into the binary.
// It is served until `make build-web` has embedded the real SvelteKit build —
// the directory contains index.html and app.css.
//
//go:embed embed_stub/*
var embedStub embed.FS

// buildFS embeds whatever `make build-web` copied into embed_build/. In a
// source checkout that directory holds only .gitkeep (build artifacts are
// gitignored), so resolveWebFS falls back to the placeholder. After
// `make build-web` the real SvelteKit build (index.html, _app/, favicon.svg)
// is compiled into the binary and served instead — a single-binary delivery.
//
//go:embed embed_build/*
var buildFS embed.FS

// resolveWebFS returns the frontend filesystem rooted at the web root, so
// HTTP paths resolve without a directory prefix (e.g. "/index.html" →
// "index.html"): the real SvelteKit build when embed_build/ contains
// index.html, otherwise the placeholder stub.
func resolveWebFS() fs.FS {
	if _, err := buildFS.Open("embed_build/index.html"); err == nil {
		sub, err := fs.Sub(buildFS, "embed_build")
		if err != nil {
			// //go:embed guarantees embed_build exists at compile time.
			panic("server: embed_build: " + err.Error())
		}
		return sub
	}
	sub, err := fs.Sub(embedStub, "embed_stub")
	if err != nil {
		// //go:embed guarantees embed_stub exists at compile time.
		panic("server: embed_stub: " + err.Error())
	}
	return sub
}

// indexHTML is the SPA fallback page served for every non-API, non-static
// route: the build's index.html when embedded, otherwise the placeholder.
var indexHTML = func() []byte {
	b, err := fs.ReadFile(resolveWebFS(), "index.html")
	if err != nil {
		panic("server: resolveWebFS index.html: " + err.Error())
	}
	return b
}()
