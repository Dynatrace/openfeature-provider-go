package dynatrace

import (
	"testing"
)

func TestProvider_Metadata(t *testing.T) {
	p := NewProvider()
	if p.Metadata().Name != "dynatrace" {
		t.Errorf("metadata name = %q, want %q", p.Metadata().Name, "dynatrace")
	}
}
