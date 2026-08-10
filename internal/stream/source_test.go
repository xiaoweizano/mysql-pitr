package stream_test

import (
	"context"
	"io"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlogtest"
	"github.com/a-shan/mysql-pitr/internal/stream"
)

type fakeStreamer struct {
	evs []binlogtest.Event
	cur int
	err error
}

func (f *fakeStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.cur >= len(f.evs) {
		return nil, io.EOF
	}
	e := f.evs[f.cur]
	f.cur++
	return &replication.BinlogEvent{
		RawData: e.Raw,
		Header:  &replication.EventHeader{EventType: e.Type, Timestamp: 1754294400},
	}, nil
}
func (f *fakeStreamer) Close() error { return nil }

func TestStreamSource_YieldsEvents(t *testing.T) {
	evs := []binlogtest.Event{
		binlogtest.MustCraft(binlogtest.CraftFDE()),
		binlogtest.MustCraft(binlogtest.CraftXID(1)),
	}
	src := stream.NewSourceWithStreamer(&fakeStreamer{evs: evs})
	defer src.Close()

	ev, err := src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, replication.FORMAT_DESCRIPTION_EVENT, ev.Header.EventType)
	ev, err = src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, replication.XID_EVENT, ev.Header.EventType)
	_, err = src.Next(context.Background())
	require.Equal(t, io.EOF, err)
}
