// Map tile caching proxy (standard library only, Go 1.21+).
//
//	go run .
//	http://localhost:8080/osm/{z}/{x}/{y}.png
//
// Features: disk cache, TTL + stale-while-revalidate, stale-if-error,
// request coalescing, ETag/304/Range via http.ServeContent, input
// validation, CORS, /stats.
package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func getEnvironmentVariable(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func loadConfig() Config {
	return Config{
		Addr:     getEnvironmentVariable("ADDR", ":8080"),
		CacheDir: getEnvironmentVariable("CACHE_DIR", "./tile-cache"),
		// Many tile servers (incl. OSM) require an identifying User-Agent.
		// UserAgent:       getEnvironmentVariable("USER_AGENT", "tile-cache-proxy/1.0 (contact: s@example.com)"),
		UserAgent:       getEnvironmentVariable("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
		TTL:             7 * 24 * time.Hour,
		Stale:           30 * 24 * time.Hour,
		UpstreamTimeout: 10 * time.Second,
		MaxTileBytes:    10 << 20,
		Providers: map[string]Provider{
			"osm": {
				URL:     "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
				Ext:     "png",
				Type:    "image/png",
				MaxZoom: 19,
			},
			// Example with subdomains:
			// "carto": {
			// 	URL:        "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
			// 	Subdomains: []string{"a", "b", "c", "d"},
			// 	Ext: "png", Type: "image/png", MaxZoom: 20,
			// },
		},
	}
}

func main() {
	config := loadConfig()
	directory, err := filepath.Abs(config.CacheDir)
	if err != nil {
		log.Fatal(err)
	}
	config.CacheDir = directory

	proxy := newServer(config)

	server := &http.Server{
		Addr:              config.Addr,
		Handler:           http.HandlerFunc(proxy.handleTile),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("tile proxy on %s, cache at %s", config.Addr, config.CacheDir)
	log.Fatal(server.ListenAndServe())
}
