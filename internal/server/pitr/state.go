package pitr

import "fmt"

// OperationState represents the current state of a PITR recovery operation
// in the v3 state machine.
type OperationState string

const (
	// StateCreated: operation record created, scan not yet started.
	StateCreated OperationState = "created"
	// StateScanning: binlog scan in progress.
	StateScanning OperationState = "scanning"
	// StateReady: scan done, transactions previewed and selectable.
	StateReady OperationState = "ready"
	// StateExecuting: selected statements being executed.
	StateExecuting OperationState = "executing"
	// StatePaused: execution paused (resumable).
	StatePaused OperationState = "paused"
	// StateDone: execution finished successfully. Terminal.
	StateDone OperationState = "done"
	// StateFailed: operation failed. Terminal.
	StateFailed OperationState = "failed"
	// StateBlocked: operation blocked by an external condition (e.g. agent
	// offline). Terminal for the state machine; a new attempt may create a new
	// operation.
	StateBlocked OperationState = "blocked" // agent 离线等外部条件
	// StateCancelled: cancelled by the operator from a cancellable state
	// (scanning / ready / paused). Terminal.
	StateCancelled OperationState = "cancelled"
)

// validTransitions defines the v3 transition table. Keys are the current
// state; values are the states that may be transitioned to:
//
//	created   -> {scanning, blocked, failed}
//	scanning  -> {ready, failed, cancelled, blocked}
//	ready     -> {executing, cancelled}
//	executing <-> paused; executing -> {done, failed}
//	paused    -> {executing, cancelled}
//
// Terminal states (done, failed, cancelled, blocked) are not included as
// sources since no transition is valid from them.
var validTransitions = map[OperationState][]OperationState{
	StateCreated:  {StateScanning, StateBlocked, StateFailed},
	StateScanning: {StateReady, StateFailed, StateCancelled, StateBlocked},
	StateReady:    {StateExecuting, StateCancelled},
	StateExecuting: {StatePaused, StateDone, StateFailed},
	StatePaused:   {StateExecuting, StateCancelled},
}

// TransitionValid checks whether moving from current state `from` to new state
// `to` is a permitted transition according to the v3 state machine.
func TransitionValid(from, to OperationState) bool {
	allowed, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// IsTerminal returns true when `state` is a terminal state (done, failed,
// cancelled, or blocked).
func IsTerminal(state OperationState) bool {
	switch state {
	case StateDone, StateFailed, StateCancelled, StateBlocked:
		return true
	}
	return false
}

// TryTransitionErr returns a descriptive error if the transition from `from`
// to `to` is invalid.
func TryTransitionErr(from, to OperationState) error {
	if !TransitionValid(from, to) {
		return fmt.Errorf("invalid state transition: %s -> %s", from, to)
	}
	return nil
}
