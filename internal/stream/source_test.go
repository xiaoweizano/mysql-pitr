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
	evs    []binlogtest.Event
	cur    int
	err    error
	closes int
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
func (f *fakeStreamer) Close() error {
	f.closes++
	return nil
}

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

// TestStreamSource_CloseIdempotent: Close 必须幂等——多次调用只触发一次底层 streamer 关闭
// （source 用 sync.Once 保护；真实 BinlogSyncer 路径同理，只 close 一次连接）。
func TestStreamSource_CloseIdempotent(t *testing.T) {
	f := &fakeStreamer{}
	src := stream.NewSourceWithStreamer(f)
	require.NoError(t, src.Close())
	require.NoError(t, src.Close())
	require.NoError(t, src.Close())
	require.Equal(t, 1, f.closes)
}
