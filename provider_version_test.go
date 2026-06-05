package dynatrace

import "testing"

func TestNormalizeSDKVersion(t *testing.T) {
	cases := map[string]string{
		"v1.2.3":                             "1.2.3",
		"1.2.3":                              "1.2.3",
		"  v0.1.0 ":                          "0.1.0",
		"(devel)":                            "",
		"":                                   "",
		"v0.0.0-20240101000000-abcdef123456": "0.0.0-20240101000000-abcdef123456",
	}
	for in, want := range cases {
		if got := normalizeSDKVersion(in); got != want {
			t.Errorf("normalizeSDKVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolveSDKVersionIsAcceptableSemver(t *testing.T) {
	v := resolveSDKVersion()
	if v == "" {
		t.Fatal("resolveSDKVersion() returned empty")
	}
	if v[0] == 'v' {
		t.Errorf("resolveSDKVersion() = %q, must not carry a leading \"v\"", v)
	}
	if v == "(devel)" || v == "unknown" {
		t.Errorf("resolveSDKVersion() = %q, must be valid semver", v)
	}
}
