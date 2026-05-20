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

func (c providerConfig) cdnurl() string {
	return c.cdnURL
}

// parseSDKKey validates the key and derives the CDN URL.
// cdnBaseURL may be empty (derive from key) or a full base URL override.
func parseSDKKey(key string, cdnBaseURL string) (providerConfig, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return providerConfig{}, errors.New("sdk key must not be blank")
	}
	if cdnBaseURL != "" && strings.TrimSpace(cdnBaseURL) == "" {
		return providerConfig{}, errors.New("cdn base URL must not be blank")
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
	if cdnBaseURL != "" {
		if !strings.HasSuffix(cdnBaseURL, "/") {
			cdnBaseURL += "/"
		}
		cdnURL = cdnBaseURL + key + ".json"
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
