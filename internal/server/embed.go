package server

import (
	"embed"
	"io/fs"
)

// embedStub bundles the placeholder frontend (embed_stub/) into the binary
// until the real SvelteKit build lands in Phase 4. The directory contains
// index.html and any static assets the placeholder needs.
//
//go:embed embed_stub/*
var embedStub embed.FS

// stubFS is the embed filesystem rooted at embed_stub/, so HTTP paths resolve
// without the embed_stub/ prefix (e.g. "/index.html" → "index.html").
var stubFS = func() fs.FS {
	sub, err := fs.Sub(embedStub, "embed_stub")
	if err != nil {
		// The //go:embed directive above guarantees embed_stub exists at
		// compile time, so this cannot fail at runtime.
		panic("server: embed_stub: " + err.Error())
	}
	return sub
}()

// placeholderIndex is the embed_stub/index.html content, served as the SPA
// fallback for every non-API, non-static route.
var placeholderIndex = func() []byte {
	b, err := embedStub.ReadFile("embed_stub/index.html")
	if err != nil {
		panic("server: embed_stub/index.html: " + err.Error())
	}
	return b
}()
