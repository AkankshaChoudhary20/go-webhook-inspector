// Command go-webhook-inspector is a small RequestBin-style webhook inspector.
//
// It hands out unique inbound endpoint URLs, captures every HTTP request
// made against them in memory, streams newly captured requests to the
// browser over Server-Sent Events, and can replay a captured request to any
// target URL. Everything lives in process memory — there is no database.
package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
)

//go:embed static
var staticFS embed.FS

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	store := NewStore()
	server := NewServer(store)

	mux := http.NewServeMux()
	server.Routes(mux)

	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("failed to load embedded static assets: %v", err)
	}
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	log.Printf("go-webhook-inspector listening on %s", *addr)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		log.Fatal(err)
	}
}
