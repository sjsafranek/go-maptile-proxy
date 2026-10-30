package main

import (
	"time"
)

type Config struct {
	Addr            string
	CacheDir        string
	UserAgent       string
	TTL             time.Duration // serve from cache without contacting upstream
	Stale           time.Duration // after TTL: serve stale + refresh in background
	UpstreamTimeout time.Duration
	MaxTileBytes    int64
	Providers       map[string]Provider
}
