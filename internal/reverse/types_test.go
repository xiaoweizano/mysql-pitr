package reverse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMaxStatementSize(t *testing.T) {
	assert.Equal(t, 16*1024, DefaultMaxStatementSize)
}
