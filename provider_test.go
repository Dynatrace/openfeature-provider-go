package dynatrace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProvider_Metadata(t *testing.T) {
	p := NewProvider()
	assert.Equal(t, "dynatrace", p.Metadata().Name)
}
