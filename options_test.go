package dynatrace

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validSDKKey = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"

func TestParseSDKKey_Valid(t *testing.T) {
	cfg, err := parseSDKKey(validSDKKey, "", "")
	require.NoError(t, err)
	assert.Equal(t, "https://cdn.us.fm.dynatrace.com/server/"+validSDKKey+".json", cfg.cdnURL)
	assert.Equal(t, "dt01.server_us_abcdef1234", cfg.publicKey())
}

func TestParseSDKKey_LeadingTrailingWhitespace(t *testing.T) {
	cfg, err := parseSDKKey("  "+validSDKKey+"\n", "", "")
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.cdnURL)
}

func TestParseSDKKey_BlankOrEmpty(t *testing.T) {
	for _, tc := range []struct{ name, key string }{{"empty", ""}, {"blank", "   "}} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSDKKey(tc.key, "", "")
			require.Error(t, err)
		})
	}
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
			_, err := parseSDKKey(key, "", "")
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
			cfg, err := parseSDKKey(tc.key, "", "")
			require.NoError(t, err)
			assert.Equal(t, "https://cdn."+tc.geo+".fm.dynatrace.com/server/"+tc.key+".json", cfg.cdnURL)
			assert.Equal(t, "https://metrics."+tc.geo+".fm.dynatrace.com/v1/metrics", cfg.metricsURL)
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
			cfg, err := parseSDKKey(validSDKKey, tc.origin, "")
			require.NoError(t, err)
			assert.Equal(t, "https://cdn.us.fm.dynatracelabs.com/server/"+validSDKKey+".json", cfg.cdnURL)
		})
	}
	cfg, err := parseSDKKey(validSDKKey, "https://cdn.example.com:443", "")
	require.NoError(t, err)
	assert.Equal(t, "https://cdn.example.com:443/server/"+validSDKKey+".json", cfg.cdnURL)
}

func TestParseSDKKey_ConfigOriginWithPathRejected(t *testing.T) {
	for _, origin := range []string{
		"https://cdn.us.fm.dynatracelabs.com/server",
		"https://example.com//",
		"https://example.com/..",
	} {
		t.Run(origin, func(t *testing.T) {
			_, err := parseSDKKey(validSDKKey, origin, "")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "path")
		})
	}
}

func TestParseSDKKey_MetricsURLDerivedFromGeo(t *testing.T) {
	cfg, err := parseSDKKey(validSDKKey, "", "")
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.us.fm.dynatrace.com/v1/metrics", cfg.metricsURL)
}

func TestParseSDKKey_WithMetricsOrigin(t *testing.T) {
	cfg, err := parseSDKKey(validSDKKey, "", "https://metrics.us.fm.dynatracelabs.com")
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.us.fm.dynatracelabs.com/v1/metrics", cfg.metricsURL)

	cfgSlash, err := parseSDKKey(validSDKKey, "", "https://metrics.us.fm.dynatracelabs.com/")
	require.NoError(t, err)
	assert.Equal(t, cfg.metricsURL, cfgSlash.metricsURL)

	cfgPort, err := parseSDKKey(validSDKKey, "", "https://metrics.example.com:443")
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.example.com:443/v1/metrics", cfgPort.metricsURL)
}

func TestParseSDKKey_BlankMetricsOriginFallsBackToGeo(t *testing.T) {
	cfg, err := parseSDKKey(validSDKKey, "", "  ")
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.us.fm.dynatrace.com/v1/metrics", cfg.metricsURL)
}

func TestParseSDKKey_MetricsOriginWithPathRejected(t *testing.T) {
	for _, origin := range []string{
		"https://example.com/v1/metrics",
		"https://example.com//",
		"https://example.com/..",
	} {
		t.Run(origin, func(t *testing.T) {
			_, err := parseSDKKey(validSDKKey, "", origin)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "path")
		})
	}
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

func TestNewProvider_WithMetricsOrigin(t *testing.T) {
	p, err := NewProvider(validSDKKey, WithMetricsOrigin("https://metrics.eu.fm.dynatracelabs.com"))
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.eu.fm.dynatracelabs.com/v1/metrics", p.config.metricsURL)
}

func TestNewProvider_BlankMetricsOriginFallsBackToGeo(t *testing.T) {
	p, err := NewProvider(validSDKKey, WithMetricsOrigin("  "))
	require.NoError(t, err)
	assert.Equal(t, "https://metrics.us.fm.dynatrace.com/v1/metrics", p.config.metricsURL)
}

func envMap(vars map[string]string) ProviderOption {
	return withLookupEnv(func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	})
}

func TestNewProvider_Env_UsedWhenNotSetInCode(t *testing.T) {
	for _, codeKey := range []string{"", "   "} {
		t.Run("code key "+strconv.Quote(codeKey), func(t *testing.T) {
			p, err := NewProvider(codeKey, envMap(map[string]string{
				envSDKKey:        validSDKKey,
				envConfigOrigin:  "https://cdn.eu.fm.dynatracelabs.com",
				envMetricsOrigin: "https://metrics.eu.fm.dynatracelabs.com",
			}))
			require.NoError(t, err)
			assert.Equal(t, validSDKKey, p.config.sdkKey)
			assert.Equal(t, "https://cdn.eu.fm.dynatracelabs.com/server/"+validSDKKey+".json", p.config.cdnURL)
			assert.Equal(t, "https://metrics.eu.fm.dynatracelabs.com/v1/metrics", p.config.metricsURL)
		})
	}
}

func TestNewProvider_Env_CodeWins(t *testing.T) {
	const envKey = "dt01.server_eu_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	p, err := NewProvider(validSDKKey,
		WithConfigOrigin("https://cdn.code.example.com"),
		WithMetricsOrigin("https://metrics.code.example.com"),
		envMap(map[string]string{
			envSDKKey:        envKey,
			envConfigOrigin:  "https://cdn.env.example.com",
			envMetricsOrigin: "https://metrics.env.example.com",
		}),
	)
	require.NoError(t, err)
	assert.Equal(t, validSDKKey, p.config.sdkKey)
	assert.Equal(t, "https://cdn.code.example.com/server/"+validSDKKey+".json", p.config.cdnURL)
	assert.Equal(t, "https://metrics.code.example.com/v1/metrics", p.config.metricsURL)
}

func TestNewProvider_Env_EmptyTreatedAsUnset(t *testing.T) {
	for _, v := range []string{"", "   "} {
		t.Run(strconv.Quote(v), func(t *testing.T) {
			p, err := NewProvider(validSDKKey, envMap(map[string]string{
				envSDKKey:        v,
				envConfigOrigin:  v,
				envMetricsOrigin: v,
			}))
			require.NoError(t, err)
			// defaults derived from the key
			assert.Equal(t, "https://cdn.us.fm.dynatrace.com/server/"+validSDKKey+".json", p.config.cdnURL)
			assert.Equal(t, "https://metrics.us.fm.dynatrace.com/v1/metrics", p.config.metricsURL)
		})
	}
}

func TestNewProvider_Env_InvalidValueFails(t *testing.T) {
	cases := []struct {
		name   string
		envVar string
		value  string
	}{
		{"bad config origin", envConfigOrigin, "https://cdn.example.com/server"},
		{"bad metrics origin", envMetricsOrigin, "ftp://metrics.example.com"},
		{"bad sdk key", envSDKKey, "dt01.client_us_abcdef1234.secretsecretsecretsecretsecret00_a1b2c3d4e5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{envSDKKey: validSDKKey, tc.envVar: tc.value}
			_, err := NewProvider("", envMap(env))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.envVar)
			assert.NotContains(t, err.Error(), "secretsecret")
		})
	}
}

func TestNewProvider_Env_MissingSDKKeyFails(t *testing.T) {
	_, err := NewProvider("  ", envMap(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), envSDKKey)
}

func TestNewProvider_Env_DefaultsToProcessEnv(t *testing.T) {
	t.Setenv(envSDKKey, validSDKKey)
	t.Setenv(envConfigOrigin, "")
	t.Setenv(envMetricsOrigin, "")
	p, err := NewProvider("")
	require.NoError(t, err)
	assert.Equal(t, validSDKKey, p.config.sdkKey)
}
