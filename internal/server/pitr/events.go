package pitr

import (
	"sync"

	"github.com/a-shan/mysql-pitr/internal/ws"
)

// subBuffer is the buffer size of each per-operation subscription channel.
// Events for a slow SSE consumer are dropped rather than blocking the agent
// read path.
const subBuffer = 16

// eventBus fans agent stream events out to per-operation subscribers (SSE
// consumers). Publish is non-blocking: slow consumers drop events, and an op
// with no subscribers is silently discarded.
//
// Usage: when an operation reaches a terminal state the operation flow calls
// Unsubscribe(opID, ch) and then closes ch; the SSE endpoint observes the
// close and cleans up. Publish never sends to a channel after Unsubscribe
// returns (both hold the same lock), so the close cannot race a send.
type eventBus struct {
	mu   sync.Mutex
	subs map[string][]chan ws.StreamEvent
}

// NewEventBus creates an empty event bus.
func NewEventBus() *eventBus {
	return &eventBus{subs: make(map[string][]chan ws.StreamEvent)}
}

// Subscribe registers a new subscriber for the given operation and returns
// its buffered receive channel. Subscriptions are keyed by operation ID;
// events published for other operations are never delivered to it.
func (b *eventBus) Subscribe(opID string) <-chan ws.StreamEvent {
	ch := make(chan ws.StreamEvent, subBuffer)
	b.mu.Lock()
	b.subs[opID] = append(b.subs[opID], ch)
	b.mu.Unlock()
	return ch
}

// Publish delivers an event to every subscriber of the operation. It never
// blocks: a send to a full subscription channel drops the event, and an op
// with no subscribers is silently discarded.
func (b *eventBus) Publish(opID string, ev ws.StreamEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs[opID] {
		select {
		case ch <- ev:
		default: // slow consumer: drop rather than block the agent path
		}
	}
}

// Unsubscribe removes a subscription from the operation. After it returns no
// Publish can target the channel again, so the caller may safely close it.
func (b *eventBus) Unsubscribe(opID string, ch <-chan ws.StreamEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subs[opID]
	for i, s := range subs {
		if s == ch {
			b.subs[opID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(b.subs[opID]) == 0 {
		delete(b.subs, opID)
	}
}
