package dynatrace

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validSDKKey = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"

func TestParseSDKKey_Valid(t *testing.T) {
	cfg, err := parseSDKKey(validSDKKey, "")
	require.NoError(t, err)
	assert.Equal(t, "https://cdn.us.fm.dynatrace.com/server/"+validSDKKey+".json", cfg.cdnURL)
	assert.Equal(t, "dt01.server_us_abcdef1234", cfg.publicKey())
}

func TestParseSDKKey_LeadingTrailingWhitespace(t *testing.T) {
	cfg, err := parseSDKKey("  "+validSDKKey+"\n", "")
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.cdnURL)
}

func TestParseSDKKey_Blank(t *testing.T) {
	_, err := parseSDKKey("   ", "")
	require.Error(t, err)
}

func TestParseSDKKey_Empty(t *testing.T) {
	_, err := parseSDKKey("", "")
	require.Error(t, err)
}

func TestParseSDKKey_InvalidFormat(t *testing.T) {
	cases := []string{
		"notakey",
		"dt01.client_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5",
		"dt01.server_US_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5",
		"dt01.server_us_short.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5",
	}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			_, err := parseSDKKey(key, "")
			require.Error(t, err)
			if lastDot := strings.LastIndex(key, "."); lastDot > 0 {
				assert.NotContains(t, err.Error(), key[lastDot+1:])
			}
		})
	}
}

func TestParseSDKKey_GeoVariants(t *testing.T) {
	cases := []struct{ key, geo string }{
		{"dt01.server_eu_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", "eu"},
		{"dt01.server_ap_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", "ap"},
		{"dt01.server_useast_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", "useast"},
	}
	for _, tc := range cases {
		t.Run(tc.geo, func(t *testing.T) {
			cfg, err := parseSDKKey(tc.key, "")
			require.NoError(t, err)
			assert.Equal(t, "https://cdn."+tc.geo+".fm.dynatrace.com/server/"+tc.key+".json", cfg.cdnURL)
		})
	}
}

func TestParseSDKKey_WithConfigOrigin(t *testing.T) {
	cases := []struct{ origin string }{
		{"https://cdn.us.fm.dynatracelabs.com"},
		{"https://cdn.us.fm.dynatracelabs.com/"},
	}
	for _, tc := range cases {
		t.Run(tc.origin, func(t *testing.T) {
			cfg, err := parseSDKKey(validSDKKey, tc.origin)
			require.NoError(t, err)
			assert.Equal(t, "https://cdn.us.fm.dynatracelabs.com/server/"+validSDKKey+".json", cfg.cdnURL)
		})
	}
}

func TestParseSDKKey_ConfigOriginWithPathRejected(t *testing.T) {
	_, err := parseSDKKey(validSDKKey, "https://cdn.us.fm.dynatracelabs.com/server")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "path")
}

func TestNewProvider_InvalidKey(t *testing.T) {
	_, err := NewProvider("not-a-valid-key")
	require.Error(t, err)
}

func TestNewProvider_WithConfigOrigin(t *testing.T) {
	p, err := NewProvider(validSDKKey, WithConfigOrigin("https://cdn.eu.fm.dynatracelabs.com"))
	require.NoError(t, err)
	assert.Equal(t, "dynatrace", p.Metadata().Name)
}
