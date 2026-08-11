package pitr

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/a-shan/mysql-pitr/internal/ws"
)

func testStreamEvent(id, kind string) ws.StreamEvent {
	return ws.StreamEvent{ID: id, Kind: kind, Data: json.RawMessage(`{"n":1}`)}
}

// TestEventBusSubscribeReceivesPublish verifies that a subscribed op receives
// published events unchanged, and that the subscription channel is buffered.
func TestEventBusSubscribeReceivesPublish(t *testing.T) {
	b := NewEventBus()
	ch := b.Subscribe("op-1")
	if cap(ch) != subBuffer {
		t.Fatalf("expected subscription buffer %d, got %d", subBuffer, cap(ch))
	}

	ev := testStreamEvent("scan-1", ws.EvTxMeta)
	b.Publish("op-1", ev)

	select {
	case got := <-ch:
		if !reflect.DeepEqual(got, ev) {
			t.Errorf("mismatch: got %+v want %+v", got, ev)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive published event")
	}
}

// TestEventBusPublishNoSubscribersDoesNotBlock verifies that Publish is
// non-blocking and silently discards events for ops with no subscribers.
func TestEventBusPublishNoSubscribersDoesNotBlock(t *testing.T) {
	b := NewEventBus()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			b.Publish("op-none", testStreamEvent("scan-x", ws.EvTxMeta))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked with no subscribers")
	}
}

// TestEventBusUnsubscribeStopsDelivery verifies that after Unsubscribe the
// subscription no longer receives events.
func TestEventBusUnsubscribeStopsDelivery(t *testing.T) {
	b := NewEventBus()
	ch := b.Subscribe("op-1")

	first := testStreamEvent("scan-1", ws.EvTxMeta)
	b.Publish("op-1", first)
	select {
	case got := <-ch:
		if !reflect.DeepEqual(got, first) {
			t.Errorf("mismatch: got %+v want %+v", got, first)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive first event")
	}

	b.Unsubscribe("op-1", ch)
	b.Publish("op-1", testStreamEvent("scan-2", ws.EvScanDone))

	select {
	case got := <-ch:
		t.Fatalf("received event after unsubscribe: %+v", got)
	case <-time.After(150 * time.Millisecond):
		// ok — nothing delivered after unsubscribe
	}
}

// TestEventBusBroadcastToMultipleSubscribers verifies that a published event
// fans out to every subscriber of the operation.
func TestEventBusBroadcastToMultipleSubscribers(t *testing.T) {
	b := NewEventBus()
	ch1 := b.Subscribe("op-1")
	ch2 := b.Subscribe("op-1")

	ev := testStreamEvent("scan-1", ws.EvTxMeta)
	b.Publish("op-1", ev)

	for _, ch := range []<-chan ws.StreamEvent{ch1, ch2} {
		select {
		case got := <-ch:
			if !reflect.DeepEqual(got, ev) {
				t.Errorf("mismatch: got %+v want %+v", got, ev)
			}
		case <-time.After(time.Second):
			t.Fatal("a subscriber did not receive the broadcast event")
		}
	}
}

// TestEventBusOperationsAreIsolated verifies that publishing to one operation
// does not leak events into subscribers of another.
func TestEventBusOperationsAreIsolated(t *testing.T) {
	b := NewEventBus()
	ch := b.Subscribe("op-1")

	b.Publish("op-2", testStreamEvent("scan-9", ws.EvTxMeta))

	select {
	case got := <-ch:
		t.Fatalf("received event for a different operation: %+v", got)
	case <-time.After(150 * time.Millisecond):
		// ok
	}
}
