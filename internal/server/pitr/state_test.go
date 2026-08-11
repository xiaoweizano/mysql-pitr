package pitr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// allStates lists every v3 state, for dead-end and coverage assertions.
var allStates = []OperationState{
	StateCreated, StateScanning, StateReady, StateExecuting, StatePaused,
	StateDone, StateFailed, StateCancelled, StateBlocked,
}

func TestTransitionValid_ValidTransitions(t *testing.T) {
	tests := []struct {
		from, to OperationState
		valid    bool
	}{
		// created -> {scanning, blocked, failed}
		{StateCreated, StateScanning, true},
		{StateCreated, StateBlocked, true},
		{StateCreated, StateFailed, true},
		// scanning -> {ready, failed, cancelled, blocked}
		{StateScanning, StateReady, true},
		{StateScanning, StateFailed, true},
		{StateScanning, StateCancelled, true},
		{StateScanning, StateBlocked, true},
		// ready -> {executing, cancelled}
		{StateReady, StateExecuting, true},
		{StateReady, StateCancelled, true},
		// executing <-> paused
		{StateExecuting, StatePaused, true},
		{StatePaused, StateExecuting, true},
		// executing -> {done, failed}
		{StateExecuting, StateDone, true},
		{StateExecuting, StateFailed, true},
		// paused -> {executing, cancelled}
		{StatePaused, StateCancelled, true},

		// Invalid transitions
		{StateCreated, StateReady, false},
		{StateCreated, StateExecuting, false},
		{StateCreated, StateDone, false},
		{StateCreated, StateCancelled, false},
		{StateScanning, StateExecuting, false},
		{StateScanning, StateDone, false},
		{StateScanning, StatePaused, false},
		{StateReady, StateScanning, false},
		{StateReady, StateDone, false},
		{StateReady, StateFailed, false},
		{StateReady, StateBlocked, false},
		{StateReady, StatePaused, false},
		{StateExecuting, StateCancelled, false},
		{StateExecuting, StateScanning, false},
		{StateExecuting, StateReady, false},
		{StatePaused, StateDone, false},
		{StatePaused, StateFailed, false},
		{StatePaused, StateReady, false},
		{StatePaused, StateBlocked, false},
		// Terminal states are dead ends (asserted in full below, sample here).
		{StateDone, StateExecuting, false},
		{StateDone, StateReady, false},
		{StateFailed, StateCancelled, false},
		{StateCancelled, StateReady, false},
		{StateBlocked, StateScanning, false},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.valid, TransitionValid(tc.from, tc.to),
			"transition %s -> %s should be valid=%v", tc.from, tc.to, tc.valid)
	}
}

func TestIsTerminal(t *testing.T) {
	// Terminal: done / failed / cancelled / blocked.
	for _, s := range []OperationState{StateDone, StateFailed, StateCancelled, StateBlocked} {
		assert.True(t, IsTerminal(s), "%s should be terminal", s)
	}
	// Non-terminal: created / scanning / ready / executing / paused.
	for _, s := range []OperationState{StateCreated, StateScanning, StateReady, StateExecuting, StatePaused} {
		assert.False(t, IsTerminal(s), "%s should not be terminal", s)
	}
}

func TestTryTransitionErr_Invalid(t *testing.T) {
	tests := []struct {
		from, to OperationState
	}{
		{StateCreated, StateReady},
		{StateReady, StateScanning},
		{StateReady, StateDone},
		{StateExecuting, StateCancelled},
		{StatePaused, StateDone},
		{StateDone, StateExecuting},
		{StateFailed, StateCancelled},
		{StateCancelled, StateReady},
		{StateBlocked, StateScanning},
	}
	for _, tc := range tests {
		err := TryTransitionErr(tc.from, tc.to)
		assert.Error(t, err, "transition %s -> %s should error", tc.from, tc.to)
		assert.Contains(t, err.Error(), "invalid state transition")
	}
}

func TestTryTransitionErr_Valid(t *testing.T) {
	tests := []struct {
		from, to OperationState
	}{
		{StateCreated, StateScanning},
		{StateScanning, StateReady},
		{StateReady, StateExecuting},
		{StateExecuting, StatePaused},
		{StatePaused, StateExecuting},
		{StateExecuting, StateDone},
		{StateScanning, StateCancelled},
		{StatePaused, StateCancelled},
	}
	for _, tc := range tests {
		err := TryTransitionErr(tc.from, tc.to)
		assert.NoError(t, err, "transition %s -> %s should not error", tc.from, tc.to)
	}
}

// TestTransitionValid_AllTerminalStatesAreDeadEnds asserts that no transition
// is valid out of any terminal state.
func TestTransitionValid_AllTerminalStatesAreDeadEnds(t *testing.T) {
	for _, terminal := range allStates {
		if !IsTerminal(terminal) {
			continue
		}
		for _, target := range allStates {
			assert.False(t, TransitionValid(terminal, target),
				"no transition from terminal state %s -> %s", terminal, target)
		}
	}
}
