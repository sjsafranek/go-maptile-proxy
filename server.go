package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	// "sync"
	"sync/atomic"
	"time"
)

// ---- server ----

func fail(resp http.ResponseWriter, code int, msg string) {
	resp.Header().Set("Access-Control-Allow-Origin", "*")
	http.Error(resp, msg, code)
}

var tileRe = regexp.MustCompile(`^/api/v1/provider/([\w-]+)/(\d{1,2})/(\d{1,7})/(\d{1,7})\.(\w+)$`)

type upstreamError struct {
	status int
}

func (self *upstreamError) Error() string {
	return fmt.Sprintf("upstream status %d", self.status)
}

type Stats struct {
	Hits, Stale, Misses, Errors, UpstreamFetches atomic.Int64
}

type Server struct {
	config Config
	client *http.Client
	flight requestManager
	stats  Stats
}

func (server *Server) upstreamURL(provider Provider, z, x, y uint64) string {
	sub := ""
	if len(provider.Subdomains) > 0 {
		sub = provider.Subdomains[rand.Intn(len(provider.Subdomains))]
	}
	return strings.NewReplacer(
		"{z}", strconv.FormatUint(z, 10),
		"{x}", strconv.FormatUint(x, 10),
		"{y}", strconv.FormatUint(y, 10),
		"{s}", sub,
	).Replace(provider.URL)
}

// fetchAndStore downloads a tile and atomically writes it to file.
// Concurrent calls for the same file share one upstream request.
func (s *Server) fetchAndStore(p Provider, z, x, y uint64, file string) error {
	return s.flight.Do(file, func() error {
		s.stats.UpstreamFetches.Add(1)

		req, err := http.NewRequest(http.MethodGet, s.upstreamURL(p, z, x, y), nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", s.config.UserAgent)
		req.Header.Set("Referer", "https://tiles.thestefanproblem.rocks")
		req.Header.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		req.Header.Set("X-Requested-With", "go-maptile-proxy")
		req.Header.Set("Accept", "image/*")

		resp, err := s.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
			return &upstreamError{resp.StatusCode}
		}

		dir := filepath.Dir(file)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		tmp, err := os.CreateTemp(dir, ".tmp-*")
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		cleanup := func() { tmp.Close(); os.Remove(tmpName) }

		n, err := io.Copy(tmp, io.LimitReader(resp.Body, s.config.MaxTileBytes+1))
		if err != nil {
			cleanup()
			return err
		}
		if n > s.config.MaxTileBytes {
			cleanup()
			return errors.New("tile too large")
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmpName)
			return err
		}
		if err := os.Chmod(tmpName, 0o644); err != nil {
			os.Remove(tmpName)
			return err
		}
		// Atomic: readers never see partial files.
		if err := os.Rename(tmpName, file); err != nil {
			os.Remove(tmpName)
			return err
		}
		return nil
	})
}

func (s *Server) serveFile(w http.ResponseWriter, r *http.Request, file string, p Provider, state string) bool {
	f, err := os.Open(file)
	if err != nil {
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return false
	}
	h := w.Header()
	h.Set("Content-Type", p.Type)
	h.Set("Cache-Control", "public, max-age=86400")
	h.Set("ETag", fmt.Sprintf(`"%x-%x"`, st.Size(), st.ModTime().UnixMilli()))
	h.Set("X-Cache", state)
	h.Set("Access-Control-Allow-Origin", "*")
	// ServeContent handles If-None-Match/If-Modified-Since (304), HEAD and Range.
	http.ServeContent(w, r, "", st.ModTime(), f)
	return true
}

func (self *Server) handleStats(resp http.ResponseWriter, _ *http.Request) {
	resp.Header().Set("Content-Type", "application/json")
	json.NewEncoder(resp).Encode(map[string]int64{
		"hits":            self.stats.Hits.Load(),
		"stale":           self.stats.Stale.Load(),
		"misses":          self.stats.Misses.Load(),
		"errors":          self.stats.Errors.Load(),
		"upstreamFetches": self.stats.UpstreamFetches.Load(),
		"inflight":        int64(self.flight.Len()),
	})
}

func (self *Server) handleProviders(resp http.ResponseWriter, _ *http.Request) {
	resp.Header().Set("Content-Type", "application/json")
	json.NewEncoder(resp).Encode(self.config.Providers)
}

func (self *Server) handleTile(resp http.ResponseWriter, req *http.Request) {

	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		fail(resp, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if req.URL.Path == "/api/v1/stats" {
		self.handleStats(resp, req)
		return
	}

	if req.URL.Path == "/api/v1/providers" {
		self.handleProviders(resp, req)
		return
	}

	m := tileRe.FindStringSubmatch(req.URL.Path)
	if m == nil {
		fail(resp, http.StatusNotFound, "use /api/v1/provider/{provider}/{z}/{x}/{y}.{ext}")
		return
	}
	name, ext := m[1], m[5]
	p, ok := self.config.Providers[name]
	if !ok || ext != p.Ext {
		fail(resp, http.StatusNotFound, "unknown provider or extension")
		return
	}
	z, _ := strconv.ParseUint(m[2], 10, 8)
	x, errX := strconv.ParseUint(m[3], 10, 32)
	y, errY := strconv.ParseUint(m[4], 10, 32)
	if errX != nil || errY != nil || int(z) > p.MaxZoom || x >= 1<<z || y >= 1<<z {
		fail(resp, http.StatusBadRequest, "tile out of range")
		return
	}

	// Path is built only from validated integers and a known provider name.
	file := filepath.Join(self.config.CacheDir, name,
		strconv.FormatUint(z, 10), strconv.FormatUint(x, 10), strconv.FormatUint(y, 10)+"."+p.Ext)

	age := time.Duration(1<<62 - 1) // "infinite" when missing
	if st, err := os.Stat(file); err == nil {
		age = time.Since(st.ModTime())
	}

	switch {
	case age <= self.config.TTL:
		if self.serveFile(resp, req, file, p, "HIT") {
			self.stats.Hits.Add(1)
			return
		}
	case age <= self.config.TTL+self.config.Stale:
		if self.serveFile(resp, req, file, p, "STALE") {
			self.stats.Stale.Add(1)
			go func() { // background refresh
				if err := self.fetchAndStore(p, z, x, y, file); err != nil {
					log.Printf("refresh %s: %v", file, err)
				}
			}()
			return
		}
	}

	self.stats.Misses.Add(1)
	if err := self.fetchAndStore(p, z, x, y, file); err != nil {
		self.stats.Errors.Add(1)
		log.Printf("fetch %s/%d/%d/%d: %v", name, z, x, y, err)
		if self.serveFile(resp, req, file, p, "STALE-ERROR") { // stale-if-error
			return
		}
		code := http.StatusBadGateway
		var ue *upstreamError
		if errors.As(err, &ue) && ue.status == http.StatusNotFound {
			code = http.StatusNotFound
		}
		fail(resp, code, "upstream error")
		return
	}
	if !self.serveFile(resp, req, file, p, "MISS") {
		fail(resp, http.StatusInternalServerError, "cache read error")
	}
}

func newServer(config Config) *Server {
	return &Server{
		config: config,
		client: &http.Client{Timeout: config.UpstreamTimeout},
		flight: newRequestManager(),
	}
}
