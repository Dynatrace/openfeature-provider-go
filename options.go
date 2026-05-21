package dynatrace

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var keyPattern = regexp.MustCompile(
	`^(dt[0-9]{2})\.(server)_([a-z]{2,})_([a-zA-Z0-9]{10,})\.([a-fA-F0-9]{32,})_([a-f0-9]{10,})$`,
)

// providerConfig holds validated configuration derived from the SDK key.
type providerConfig struct {
	sdkKey string
	cdnURL string
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
	if configOrigin != "" && strings.TrimSpace(configOrigin) == "" {
		return providerConfig{}, errors.New("config origin must not be blank")
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
	if configOrigin != "" {
		origin := strings.TrimRight(configOrigin, "/")
		cdnURL = origin + "/" + platform + "/" + key + ".json"
	} else {
		cdnURL = "https://cdn." + geo + ".fm.dynatrace.com/" + platform + "/" + key + ".json"
	}

	return providerConfig{sdkKey: key, cdnURL: cdnURL}, nil
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
