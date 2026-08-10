package stream

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

// streamer 抽象 binlogsyncer 的事件拉取，便于测试注入 fake。
type streamer interface {
	GetEvent(ctx context.Context) (*replication.BinlogEvent, error)
	Close() error
}

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	ServerID uint32
	Flavor   string
	SyncPos  mysql.Position
	SyncGTID mysql.GTIDSet
}

// binlogStreamer 适配 replication.BinlogStreamer。
type binlogStreamer struct{ s *replication.BinlogStreamer }

func (b *binlogStreamer) GetEvent(ctx context.Context) (*replication.BinlogEvent, error) {
	return b.s.GetEvent(ctx)
}
func (b *binlogStreamer) Close() error { return nil }

type source struct {
	st streamer
}

func (s *source) Next(ctx context.Context) (*replication.BinlogEvent, error) {
	return s.st.GetEvent(ctx)
}
func (s *source) Close() error { return s.st.Close() }

// NewSource 创建真实 binlogsyncer 驱动的事件源。
func NewSource(cfg Config) (binlog.Source, error) {
	flavor := cfg.Flavor
	if flavor == "" {
		flavor = mysql.MySQLFlavor
	}
	sync := replication.BinlogSyncerConfig{
		ServerID:        cfg.ServerID,
		Flavor:          flavor,
		Host:            cfg.Host,
		Port:            uint16(cfg.Port),
		User:            cfg.User,
		Password:        cfg.Password,
		RawModeEnabled:  false, // 解析模式；BinlogEvent.RawData 始终可用，归档照常还原
	}
	syncer := replication.NewBinlogSyncer(sync)
	var st *replication.BinlogStreamer
	var err error
	if cfg.SyncGTID != nil && !cfg.SyncGTID.IsEmpty() {
		st, err = syncer.StartSyncGTID(cfg.SyncGTID)
	} else {
		st, err = syncer.StartSync(cfg.SyncPos)
	}
	if err != nil {
		return nil, fmt.Errorf("stream: start sync: %w", err)
	}
	return &source{st: &binlogStreamer{s: st}}, nil
}

func NewSourceWithStreamer(st streamer) binlog.Source {
	return &source{st: st}
}
