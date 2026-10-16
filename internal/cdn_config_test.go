package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSSEURL(t *testing.T) {
	const fallback = "https://cdn.example.test"

	tests := []struct {
		name       string
		body       string
		want       string
		advertised bool
	}{
		{
			name:       "flat url",
			body:       `{"eventStreams":[{"type":"sse","url":"https://stream.example.test/sse?k=v"}]}`,
			want:       "https://stream.example.test/sse?k=v",
			advertised: true,
		},
		{
			name:       "structured endpoint with its own origin",
			body:       `{"eventStreams":[{"type":"sse","endpoint":{"origin":"https://sse.example.test","requestUri":"/sse"}}]}`,
			want:       "https://sse.example.test/sse",
			advertised: true,
		},
		{
			name:       "structured endpoint falls back to the CDN origin",
			body:       `{"eventStreams":[{"type":"sse","endpoint":{"requestUri":"/sse"}}]}`,
			want:       "https://cdn.example.test/sse",
			advertised: true,
		},
		{
			name:       "origin and request uri are joined with exactly one slash",
			body:       `{"eventStreams":[{"type":"sse","endpoint":{"origin":"https://sse.example.test/","requestUri":"sse?k=v"}}]}`,
			want:       "https://sse.example.test/sse?k=v",
			advertised: true,
		},
		{
			name:       "flat url wins over a structured endpoint",
			body:       `{"eventStreams":[{"type":"sse","url":"https://flat.example.test/sse","endpoint":{"requestUri":"/other"}}]}`,
			want:       "https://flat.example.test/sse",
			advertised: true,
		},
		{
			name:       "first sse entry wins past other types",
			body:       `{"eventStreams":[{"type":"ws","url":"wss://x.example.test"},{"type":"sse","url":"https://first.example.test/sse"},{"type":"sse","url":"https://second.example.test/sse"}]}`,
			want:       "https://first.example.test/sse",
			advertised: true,
		},
		{
			name:       "unusable entry is advertised but has no url",
			body:       `{"eventStreams":[{"type":"sse"}]}`,
			want:       "",
			advertised: true,
		},
		{
			name:       "scheme other than http(s) is unusable",
			body:       `{"eventStreams":[{"type":"sse","url":"ftp://s.example.test/sse"}]}`,
			want:       "",
			advertised: true,
		},
		{
			name: "no sse entry",
			body: `{"eventStreams":[{"type":"ws","url":"https://x.example.test"}]}`,
		},
		{
			name: "invalid json",
			body: `{"eventStreams":[`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, advertised := ParseSSEURL(tt.body, fallback)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.advertised, advertised)
		})
	}
}

func TestOriginOf(t *testing.T) {
	assert.Equal(t, "https://cdn.us.fm.dynatrace.com", OriginOf("https://cdn.us.fm.dynatrace.com/server/key.json"))
	assert.Equal(t, "http://127.0.0.1:8080", OriginOf("http://127.0.0.1:8080/server/key.json"))
	assert.Equal(t, "", OriginOf("/server/key.json"))
}
