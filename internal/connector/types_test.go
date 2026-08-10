package connector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPreflightResult_EnsureOK(t *testing.T) {
	assert.NoError(t, (PreflightResult{Status: PreflightPass}).EnsureOK())
	assert.NoError(t, (PreflightResult{Status: PreflightWarn}).EnsureOK())
	assert.ErrorContains(t, (PreflightResult{Status: PreflightFail}).EnsureOK(), "preflight FAILED")
}
