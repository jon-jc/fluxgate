// Package dashboard serves the browser workspace alongside the query API.
// Assets are embedded so containers and native binaries need no Node runtime.
package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/index.html web/app.js web/core.js web/demo.js web/dashboard.css web/favicon.svg
var assets embed.FS

// Handler serves only the embedded application. API credentials remain in the
// browser's memory; all tenant data still passes through authenticated routes.
func Handler() http.Handler {
	files, err := fs.Sub(assets, "web")
	if err != nil {
		panic(err)
	}
	server := http.StripPrefix("/dashboard/", http.FileServer(http.FS(files)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' https:; img-src 'self' data:; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		server.ServeHTTP(w, r)
	})
}
