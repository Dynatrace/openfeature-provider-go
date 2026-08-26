package dynatrace

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var keyPattern = regexp.MustCompile(
	`^(dt[0-9]{2})\.(server)_([a-z]{2,})_([a-zA-Z0-9]{10,})\.([a-fA-F0-9]{32,})_([a-f0-9]{10,})$`,
)

// providerConfig holds validated configuration derived from the SDK key.
type providerConfig struct {
	sdkKey     string
	cdnURL     string
	metricsURL string
}

func (c providerConfig) publicKey() string {
	return publicKeySegment(c.sdkKey)
}

// parseSDKKey validates the key and derives the CDN URL.
// configOrigin may be empty (derive from key) or an origin override (scheme, host, port — no path).
func parseSDKKey(key string, configOrigin string) (providerConfig, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return providerConfig{}, errors.New("sdk key must not be blank")
	}
	configOrigin = strings.TrimSpace(configOrigin)
	var origin string
	if configOrigin != "" {
		var err error
		origin, err = normalizeConfigOrigin(configOrigin)
		if err != nil {
			return providerConfig{}, err
		}
	}
	m := keyPattern.FindStringSubmatch(key)
	if m == nil {
		return providerConfig{}, fmt.Errorf(
			"invalid sdk key format (public: %q); expected dt{NN}.server_{geo}_{rand}.{secret}_{sha}",
			publicKeySegment(key),
		)
	}
	geo := m[3]
	platform := m[2]

	var cdnURL string
	if origin != "" {
		cdnURL = origin + "/" + platform + "/" + key + ".json"
	} else {
		cdnURL = "https://cdn." + geo + ".fm.dynatrace.com/" + platform + "/" + key + ".json"
	}

	// The metrics ingest URL is always derived from the geo segment. A config-origin override
	// targets only the CDN; metrics are overridden separately via WithMetricsURL.
	metricsURL := "https://metrics." + geo + ".fm.dynatrace.com/v1/metrics"

	return providerConfig{sdkKey: key, cdnURL: cdnURL, metricsURL: metricsURL}, nil
}

// normalizeConfigOrigin validates that value is a bare origin and returns it in
// canonical form. An origin is a scheme, host, and optional port — nothing else.
// Rebuilding from the parsed parts (rather than trimming the raw string) keeps a
// query or fragment from silently corrupting the appended path segments, which
// would surface only as a 404 on the first config fetch.
func normalizeConfigOrigin(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("config origin is not a valid URL; got %q: %w", value, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("config origin must use http or https; got %q", value)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("config origin must include a host; got %q", value)
	}
	if p := strings.TrimRight(u.Path, "/"); p != "" {
		return "", fmt.Errorf("config origin must not include a path; got %q", value)
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", fmt.Errorf("config origin must not include a query string; got %q", value)
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("config origin must not include a fragment; got %q", value)
	}
	// u.Host rather than Hostname() + Port(): it preserves the brackets around an
	// IPv6 literal, which Hostname() strips. Host excludes any user info, which is
	// dropped rather than carried into the request URL.
	return scheme + "://" + strings.ToLower(u.Host), nil
}

func publicKeySegment(key string) string {
	// Only trust the structure if the key has exactly two dots (expected format).
	// For anything else, redact entirely to avoid leaking an unknown private segment.
	first := strings.Index(key, ".")
	last := strings.LastIndex(key, ".")
	if first <= 0 || first == last {
		return "<redacted>"
	}
	return key[:last]
}
