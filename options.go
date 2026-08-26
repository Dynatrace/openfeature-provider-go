package dynatrace

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

const metricsPath = "/v1/metrics"

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

// parseSDKKey validates the key and derives the CDN and metrics URLs.
// configOrigin and metricsOrigin may be empty (derive from key) or an origin override
// (scheme, host, optional port; no path).
func parseSDKKey(key string, configOrigin string, metricsOrigin string) (providerConfig, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return providerConfig{}, errors.New("sdk key must not be blank")
	}
	configOrigin = strings.TrimSpace(configOrigin)
	var origin string
	if configOrigin != "" {
		var err error
		origin, err = normalizeOrigin(configOrigin, "config origin")
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

	metricsURL, err := resolveMetricsURL(geo, metricsOrigin)
	if err != nil {
		return providerConfig{}, err
	}

	return providerConfig{sdkKey: key, cdnURL: cdnURL, metricsURL: metricsURL}, nil
}

func resolveMetricsURL(geo string, metricsOrigin string) (string, error) {
	metricsOrigin = strings.TrimSpace(metricsOrigin)
	if metricsOrigin == "" {
		return "https://metrics." + geo + ".fm.dynatrace.com" + metricsPath, nil
	}
	origin, err := normalizeOrigin(metricsOrigin, "metrics origin")
	if err != nil {
		return "", err
	}
	return origin + metricsPath, nil
}

// normalizeOrigin validates that value is a bare origin and returns it in
// canonical form. An origin is a scheme, host, and optional port; nothing else.
// Rebuilding from the parsed parts (rather than trimming the raw string) keeps a
// query or fragment from silently corrupting appended path segments, which
// would surface only as a 404 on the first request.
func normalizeOrigin(value, field string) (string, error) {
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%s is not a valid URL; got %q: %w", field, value, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("%s must use http or https; got %q", field, value)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("%s must include a host; got %q", field, value)
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("%s must not include a path; got %q", field, value)
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", fmt.Errorf("%s must not include a query string; got %q", field, value)
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("%s must not include a fragment; got %q", field, value)
	}
	if u.User != nil {
		return "", fmt.Errorf("%s must not include user info; got %q", field, value)
	}
	// u.Host rather than Hostname() + Port(): it preserves the brackets around an
	// IPv6 literal, which Hostname() strips. User info is already rejected above, so
	// Host is exactly the host and optional port.
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
