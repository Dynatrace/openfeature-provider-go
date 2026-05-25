package dynatrace

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// SDK key format: {token}.server_{geo}_{publicRandom}.{secret}_{sha1}
var keyPattern = regexp.MustCompile(
	`^(dt[0-9]{2})\.(server)_([a-z]{2,})_([a-zA-Z0-9]{10,})\.([a-fA-F0-9]{32,})_([a-f0-9]{10,})$`,
)

// Options holds validated provider configuration derived from the SDK key.
type Options struct {
	sdkKey string
	cdnURL string
	optErr error
}

// Option is a functional option for Options.
type Option func(*Options)

// WithCDNBaseURL overrides the derived CDN base URL. For dev/labs environments.
// A trailing slash is appended automatically if missing.
func WithCDNBaseURL(base string) Option {
	return func(o *Options) {
		if strings.TrimSpace(base) == "" {
			o.optErr = errors.New("WithCDNBaseURL: base URL must not be blank")
			return
		}
		if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		o.cdnURL = base + o.sdkKey + ".json"
	}
}

// FromSDKKey validates the SDK key and returns an Options value.
// Functional options are applied after key validation.
func FromSDKKey(key string, opts ...Option) (Options, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Options{}, errors.New("sdk key must not be blank")
	}
	m := keyPattern.FindStringSubmatch(key)
	if m == nil {
		publicPart := publicKeySegment(key)
		return Options{}, fmt.Errorf(
			"invalid sdk key format (public: %q); expected dt{NN}.server_{geo}_{rand}.{secret}_{sha}",
			publicPart,
		)
	}

	geo := m[3]
	platform := m[2]
	o := Options{
		sdkKey: key,
		cdnURL: "https://cdn." + geo + ".fm.dynatrace.com/" + platform + "/" + key + ".json",
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.optErr != nil {
		return Options{}, o.optErr
	}
	return o, nil
}

// PublicKey returns the log-safe prefix (everything before the last dot).
// Never exposes the private secret segment.
func (o Options) PublicKey() string {
	return publicKeySegment(o.sdkKey)
}

// CDNURL returns the full CDN URL for this key.
func (o Options) CDNURL() string {
	return o.cdnURL
}

func publicKeySegment(key string) string {
	idx := strings.LastIndex(key, ".")
	if idx <= 0 {
		return "<redacted>"
	}
	return key[:idx]
}
