package dynatrace

import (
	"strings"
	"testing"
)

func TestFromSDKKey_Valid(t *testing.T) {
	key := "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	opts, err := FromSDKKey(key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.CDNURL() != "https://cdn.us.fm.dynatrace.com/server/"+key+".json" {
		t.Errorf("unexpected CDN URL: %s", opts.CDNURL())
	}
	if opts.PublicKey() != "dt01.server_us_abcdef1234" {
		t.Errorf("unexpected public key: %s", opts.PublicKey())
	}
}

func TestFromSDKKey_LeadingTrailingWhitespace(t *testing.T) {
	key := "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	opts, err := FromSDKKey("  " + key + "\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.CDNURL() == "" {
		t.Error("expected non-empty CDN URL")
	}
}

func TestFromSDKKey_Blank(t *testing.T) {
	_, err := FromSDKKey("   ")
	if err == nil {
		t.Fatal("expected error for blank key")
	}
}

func TestFromSDKKey_Empty(t *testing.T) {
	_, err := FromSDKKey("")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestFromSDKKey_InvalidFormat(t *testing.T) {
	cases := []string{
		"notakey",
		"dt01.client_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", // wrong platform
		"dt01.server_US_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", // geo uppercase
		"dt01.server_us_short.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5",       // publicRandom too short
	}
	for _, key := range cases {
		t.Run(key, func(t *testing.T) {
			_, err := FromSDKKey(key)
			if err == nil {
				t.Errorf("expected error for key %q", key)
			}
			// error message must not contain the private segment (after last dot)
			if lastDot := strings.LastIndex(key, "."); lastDot > 0 {
				privateSegment := key[lastDot+1:]
				if strings.Contains(err.Error(), privateSegment) {
					t.Errorf("error message leaks private segment: %v", err)
				}
			}
		})
	}
}

func TestFromSDKKey_GeoVariants(t *testing.T) {
	cases := []struct {
		key string
		geo string
	}{
		{"dt01.server_eu_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", "eu"},
		{"dt01.server_ap_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", "ap"},
		{"dt01.server_useast_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5", "useast"},
	}
	for _, tc := range cases {
		opts, err := FromSDKKey(tc.key)
		if err != nil {
			t.Errorf("key %s: unexpected error: %v", tc.key, err)
			continue
		}
		expected := "https://cdn." + tc.geo + ".fm.dynatrace.com/server/" + tc.key + ".json"
		if opts.CDNURL() != expected {
			t.Errorf("key %s: CDN URL = %s, want %s", tc.key, opts.CDNURL(), expected)
		}
	}
}

func TestFromSDKKey_WithCDNBaseURL(t *testing.T) {
	key := "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	opts, err := FromSDKKey(key, WithCDNBaseURL("https://cdn.us.fm.dynatracelabs.com/server"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "https://cdn.us.fm.dynatracelabs.com/server/" + key + ".json"
	if opts.CDNURL() != want {
		t.Errorf("CDN URL = %s, want %s", opts.CDNURL(), want)
	}
}

func TestFromSDKKey_WithCDNBaseURL_Blank(t *testing.T) {
	key := "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	for _, base := range []string{"", "   "} {
		_, err := FromSDKKey(key, WithCDNBaseURL(base))
		if err == nil {
			t.Errorf("expected error for blank base URL %q", base)
		}
	}
}

func TestFromSDKKey_WithCDNBaseURL_TrailingSlash(t *testing.T) {
	key := "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	opts, err := FromSDKKey(key, WithCDNBaseURL("https://cdn.us.fm.dynatracelabs.com/server/"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "https://cdn.us.fm.dynatracelabs.com/server/" + key + ".json"
	if opts.CDNURL() != want {
		t.Errorf("CDN URL = %s, want %s", opts.CDNURL(), want)
	}
}
