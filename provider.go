package main

type Provider struct {
	URL        string   // placeholders: {z} {x} {y} {s}
	Subdomains []string // optional, for {s}
	Ext        string
	Type       string
	MaxZoom    int
}
