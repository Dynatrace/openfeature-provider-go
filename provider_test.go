package dynatrace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProvider_Metadata(t *testing.T) {
	key := "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
	opts, err := FromSDKKey(key)
	require.NoError(t, err)
	p := NewProvider(opts)
	assert.Equal(t, "dynatrace", p.Metadata().Name)
}
