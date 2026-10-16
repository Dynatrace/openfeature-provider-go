package internal

import (
	"encoding/json"
	"net/url"
	"strings"
)

// streamTypeSSE is the eventStreams[].type value that identifies the SSE change-notification stream.
const streamTypeSSE = "sse"

// cdnConfig is the part of the CDN config body this provider reads; the flag definitions are handed to
// flagd-core as the raw body and never parsed here. Unknown fields are ignored so the server can add
// new eventStreams attributes without breaking older SDKs.
type cdnConfig struct {
	EventStreams []eventStream `json:"eventStreams"`
}

type eventStream struct {
	Type     string    `json:"type"`
	URL      string    `json:"url"`
	Endpoint *endpoint `json:"endpoint"`
}

// endpoint is the structured alternative to the flat url; origin defaults to the CDN origin when blank.
type endpoint struct {
	Origin     string `json:"origin"`
	RequestURI string `json:"requestUri"`
}

// ParseSSEURL returns the SSE stream URL advertised by the CDN config body, or "" when the provider
// should poll only: no sse entry, an unusable entry, a URL that is not http(s), or a body that is not JSON.
// advertised reports whether the config has an sse entry at all, so a caller can tell an unusable entry
// ("" and true) from no entry ("" and false).
//
// The first entry with type "sse" decides. A non-blank flat url wins; otherwise a structured endpoint is
// resolved as origin + requestUri, with fallbackOrigin standing in for a blank origin.
func ParseSSEURL(body, fallbackOrigin string) (streamURL string, advertised bool) {
	var cfg cdnConfig
	if err := json.Unmarshal([]byte(body), &cfg); err != nil {
		return "", false
	}
	for _, stream := range cfg.EventStreams {
		if stream.Type != streamTypeSSE {
			continue
		}
		return validStreamURL(resolveStreamURL(stream, fallbackOrigin)), true
	}
	return "", false
}

func resolveStreamURL(stream eventStream, fallbackOrigin string) string {
	if strings.TrimSpace(stream.URL) != "" {
		return strings.TrimSpace(stream.URL)
	}
	ep := stream.Endpoint
	if ep == nil || strings.TrimSpace(ep.RequestURI) == "" {
		return ""
	}
	origin := strings.TrimSpace(ep.Origin)
	if origin == "" {
		origin = fallbackOrigin
	}
	if origin == "" {
		return ""
	}
	requestURI := strings.TrimSpace(ep.RequestURI)
	if !strings.HasPrefix(requestURI, "/") {
		requestURI = "/" + requestURI
	}
	return strings.TrimRight(origin, "/") + requestURI
}

// validStreamURL returns raw when it is an absolute http(s) URL with a host, and "" otherwise.
func validStreamURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	if scheme := strings.ToLower(u.Scheme); scheme != "http" && scheme != "https" {
		return ""
	}
	return raw
}

// OriginOf returns the scheme://host[:port] of a URL, or "" when it cannot be parsed. It is the origin a
// structured SSE endpoint falls back to when it names none.
func OriginOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}
