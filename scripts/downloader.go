// Map tile downloader (XYZ / slippy-map scheme) using only the Go standard library.
//
// Build:   go mod init tiledl && go build -o tiledl .
// Example: ./tiledl -url "https://tile.example.com/{z}/{x}/{y}.png" \
//                   -bbox "-122.52,37.70,-122.35,37.83" -zoom 10-14 -out ./tiles
//
// URL placeholders: {z} {x} {y}  {-y} (TMS-flipped y)  {s} (rotates through -subdomains)
//
// IMPORTANT: Respect the tile provider's terms of use. For example, the public
// OpenStreetMap tile servers prohibit bulk downloading. Use your own tile server,
// a provider that permits offline caching, or a commercial plan.
package main
 
import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
 
type Tile struct{ Z, X, Y int }
 
type config struct {
	urlTmpl    string
	outDir     string
	ext        string
	userAgent  string
	subdomains []string
	workers    int
	retries    int
	timeout    time.Duration
	rate       float64
	overwrite  bool
}
 
type stats struct {
	total, done, downloaded, skipped, missing, failed, bytes int64
}
 
func main() {
	var (
		urlTmpl    = flag.String("url", "", "tile URL template, e.g. https://host/{z}/{x}/{y}.png (required)")
		bboxStr    = flag.String("bbox", "", "bounding box: minLon,minLat,maxLon,maxLat (required)")
		zoomStr    = flag.String("zoom", "0-5", "zoom level or range, e.g. 12 or 10-14")
		outDir     = flag.String("out", "tiles", "output directory")
		ext        = flag.String("ext", "png", "file extension for saved tiles")
		workers    = flag.Int("workers", 8, "concurrent downloads")
		retries    = flag.Int("retries", 3, "retries per tile")
		timeout    = flag.Duration("timeout", 20*time.Second, "HTTP timeout per request")
		rate       = flag.Float64("rate", 0, "max requests per second across all workers (0 = unlimited)")
		userAgent  = flag.String("ua", "tiledl/1.0 (+https://example.com/contact)", "User-Agent header (set a real contact!)")
		subdomains = flag.String("subdomains", "a,b,c", "comma-separated values for {s}")
		overwrite  = flag.Bool("overwrite", false, "re-download tiles that already exist")
		maxTiles   = flag.Int64("max-tiles", 10000, "refuse to run if more tiles than this (safety limit; 0 = no limit)")
		dryRun     = flag.Bool("dry-run", false, "only count tiles, do not download")
	)
	flag.Parse()
 
	if *urlTmpl == "" || *bboxStr == "" {
		flag.Usage()
		os.Exit(2)
	}
	minLon, minLat, maxLon, maxLat, err := parseBBox(*bboxStr)
	fatalIf(err)
	minZ, maxZ, err := parseZoom(*zoomStr)
	fatalIf(err)
 
	cfg := config{
		urlTmpl:    *urlTmpl,
		outDir:     *outDir,
		ext:        strings.TrimPrefix(*ext, "."),
		userAgent:  *userAgent,
		subdomains: splitNonEmpty(*subdomains),
		workers:    max(1, *workers),
		retries:    max(0, *retries),
		timeout:    *timeout,
		rate:       *rate,
		overwrite:  *overwrite,
	}
 
	total := countTiles(minLon, minLat, maxLon, maxLat, minZ, maxZ)
	fmt.Printf("Tiles to fetch: %d (zoom %d-%d)\n", total, minZ, maxZ)
	if *maxTiles > 0 && total > *maxTiles {
		fatalIf(fmt.Errorf("%d tiles exceeds -max-tiles=%d; raise the limit if you're sure", total, *maxTiles))
	}
	if *dryRun {
		return
	}
 
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
 
	st := &stats{total: total}
	run(ctx, cfg, st, minLon, minLat, maxLon, maxLat, minZ, maxZ)
 
	fmt.Printf("\nDone: %d downloaded, %d skipped, %d missing (404), %d failed, %.1f MB\n",
		st.downloaded, st.skipped, st.missing, st.failed, float64(st.bytes)/1e6)
	if ctx.Err() != nil {
		fmt.Println("Interrupted; re-run to resume (existing tiles are skipped).")
	}
	if st.failed > 0 {
		os.Exit(1)
	}
}
 
func run(ctx context.Context, cfg config, st *stats, minLon, minLat, maxLon, maxLat float64, minZ, maxZ int) {
	client := &http.Client{
		Timeout: cfg.timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.workers,
			IdleConnTimeout:     60 * time.Second,
		},
	}
 
	tiles := make(chan Tile, cfg.workers*2)
 
	// Optional global rate limiter.
	var limiter <-chan time.Time
	if cfg.rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / cfg.rate))
		defer t.Stop()
		limiter = t.C
	}
 
	// Producer.
	go func() {
		defer close(tiles)
		for z := minZ; z <= maxZ; z++ {
			x0, y0, x1, y1 := tileRange(minLon, minLat, maxLon, maxLat, z)
			for x := x0; x <= x1; x++ {
				for y := y0; y <= y1; y++ {
					select {
					case tiles <- Tile{z, x, y}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
 
	// Progress printer.
	progDone := make(chan struct{})
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				printProgress(st)
			case <-progDone:
				printProgress(st)
				return
			}
		}
	}()
 
	var wg sync.WaitGroup
	var counter uint64
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tiles {
				if ctx.Err() != nil {
					return
				}
				path := tilePath(cfg, t)
				if !cfg.overwrite {
					if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
						atomic.AddInt64(&st.skipped, 1)
						atomic.AddInt64(&st.done, 1)
						continue
					}
				}
				if limiter != nil {
					select {
					case <-limiter:
					case <-ctx.Done():
						return
					}
				}
				sub := ""
				if len(cfg.subdomains) > 0 {
					sub = cfg.subdomains[atomic.AddUint64(&counter, 1)%uint64(len(cfg.subdomains))]
				}
				n, err := downloadWithRetry(ctx, client, cfg, t, sub, path)
				switch {
				case err == nil:
					atomic.AddInt64(&st.downloaded, 1)
					atomic.AddInt64(&st.bytes, n)
				case errors.Is(err, errNotFound):
					atomic.AddInt64(&st.missing, 1)
				case ctx.Err() != nil:
					return
				default:
					atomic.AddInt64(&st.failed, 1)
					fmt.Fprintf(os.Stderr, "\nfailed %d/%d/%d: %v\n", t.Z, t.X, t.Y, err)
				}
				atomic.AddInt64(&st.done, 1)
			}
		}()
	}
	wg.Wait()
	close(progDone)
	time.Sleep(50 * time.Millisecond) // let the final progress line print
}
 
var errNotFound = errors.New("tile not found")
 
func downloadWithRetry(ctx context.Context, c *http.Client, cfg config, t Tile, sub, path string) (int64, error) {
	var lastErr error
	for attempt := 0; attempt <= cfg.retries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt-1)) * 500 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		n, retry, err := downloadOnce(ctx, c, cfg, t, sub, path)
		if err == nil {
			return n, nil
		}
		lastErr = err
		if !retry {
			return 0, err
		}
	}
	return 0, lastErr
}
 
// downloadOnce returns (bytes, shouldRetry, error).
func downloadOnce(ctx context.Context, c *http.Client, cfg config, t Tile, sub, path string) (int64, bool, error) {
	url := buildURL(cfg.urlTmpl, t, sub)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("User-Agent", cfg.userAgent)
 
	resp, err := c.Do(req)
	if err != nil {
		return 0, true, err
	}
	defer resp.Body.Close()
 
	switch {
	case resp.StatusCode == http.StatusNotFound:
		io.Copy(io.Discard, resp.Body)
		return 0, false, errNotFound
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		io.Copy(io.Discard, resp.Body)
		return 0, true, fmt.Errorf("HTTP %d", resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		io.Copy(io.Discard, resp.Body)
		return 0, false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
 
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return 0, false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tile-*")
	if err != nil {
		return 0, false, err
	}
	tmpName := tmp.Name()
	n, copyErr := io.Copy(tmp, resp.Body)
	closeErr := tmp.Close()
	if copyErr != nil || closeErr != nil {
		os.Remove(tmpName)
		return 0, true, errors.Join(copyErr, closeErr)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return 0, false, err
	}
	return n, false, nil
}
 
// ---------- tile math ----------
 
func lonToX(lon float64, z int) int {
	n := math.Pow(2, float64(z))
	x := int(math.Floor((lon + 180.0) / 360.0 * n))
	return clamp(x, 0, int(n)-1)
}
 
func latToY(lat float64, z int) int {
	lat = math.Max(-85.05112878, math.Min(85.05112878, lat))
	n := math.Pow(2, float64(z))
	r := lat * math.Pi / 180.0
	y := int(math.Floor((1.0 - math.Asinh(math.Tan(r))/math.Pi) / 2.0 * n))
	return clamp(y, 0, int(n)-1)
}
 
// tileRange returns inclusive x/y ranges covering the bbox at zoom z.
func tileRange(minLon, minLat, maxLon, maxLat float64, z int) (x0, y0, x1, y1 int) {
	x0, x1 = lonToX(minLon, z), lonToX(maxLon, z)
	y0, y1 = latToY(maxLat, z), latToY(minLat, z) // y grows southward
	return
}
 
func countTiles(minLon, minLat, maxLon, maxLat float64, minZ, maxZ int) int64 {
	var total int64
	for z := minZ; z <= maxZ; z++ {
		x0, y0, x1, y1 := tileRange(minLon, minLat, maxLon, maxLat, z)
		total += int64(x1-x0+1) * int64(y1-y0+1)
	}
	return total
}
 
// ---------- helpers ----------
 
func buildURL(tmpl string, t Tile, sub string) string {
	tmsY := (1 << uint(t.Z)) - 1 - t.Y
	r := strings.NewReplacer(
		"{z}", strconv.Itoa(t.Z),
		"{x}", strconv.Itoa(t.X),
		"{y}", strconv.Itoa(t.Y),
		"{-y}", strconv.Itoa(tmsY),
		"{s}", sub,
	)
	return r.Replace(tmpl)
}
 
func tilePath(cfg config, t Tile) string {
	return filepath.Join(cfg.outDir, strconv.Itoa(t.Z), strconv.Itoa(t.X),
		fmt.Sprintf("%d.%s", t.Y, cfg.ext))
}
 
func parseBBox(s string) (minLon, minLat, maxLon, maxLat float64, err error) {
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return 0, 0, 0, 0, errors.New("bbox must be minLon,minLat,maxLon,maxLat")
	}
	v := make([]float64, 4)
	for i, p := range parts {
		if v[i], err = strconv.ParseFloat(strings.TrimSpace(p), 64); err != nil {
			return 0, 0, 0, 0, fmt.Errorf("bad bbox value %q", p)
		}
	}
	if v[0] >= v[2] || v[1] >= v[3] {
		return 0, 0, 0, 0, errors.New("bbox min must be less than max")
	}
	return v[0], v[1], v[2], v[3], nil
}
 
func parseZoom(s string) (minZ, maxZ int, err error) {
	lo, hi, found := strings.Cut(s, "-")
	if minZ, err = strconv.Atoi(strings.TrimSpace(lo)); err != nil {
		return 0, 0, fmt.Errorf("bad zoom %q", s)
	}
	maxZ = minZ
	if found {
		if maxZ, err = strconv.Atoi(strings.TrimSpace(hi)); err != nil {
			return 0, 0, fmt.Errorf("bad zoom %q", s)
		}
	}
	if minZ < 0 || maxZ > 24 || minZ > maxZ {
		return 0, 0, errors.New("zoom must satisfy 0 <= min <= max <= 24")
	}
	return minZ, maxZ, nil
}
 
func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
 
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
 
func printProgress(st *stats) {
	done := atomic.LoadInt64(&st.done)
	pct := 0.0
	if st.total > 0 {
		pct = float64(done) / float64(st.total) * 100
	}
	fmt.Printf("\r%d/%d (%.1f%%)  ok:%d skip:%d 404:%d fail:%d  ",
		done, st.total, pct,
		atomic.LoadInt64(&st.downloaded), atomic.LoadInt64(&st.skipped),
		atomic.LoadInt64(&st.missing), atomic.LoadInt64(&st.failed))
}
 
func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}