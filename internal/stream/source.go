package stream

import (
	"context"
	"fmt"
	"sync"

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
	st        streamer
	syncer    *replication.BinlogSyncer // NewSourceWithStreamer 注入时为空
	closeOnce sync.Once
}

func (s *source) Next(ctx context.Context) (*replication.BinlogEvent, error) {
	return s.st.GetEvent(ctx)
}

// Close 关闭事件源：先关 streamer，再关 BinlogSyncer。
// BinlogStreamer 本身没有导出 Close（go-mysql 只通过 BinlogSyncer.Close 真正
// 断连接、取消 ctx 并 wg.Wait 收尾 onStream goroutine），因此 source 必须持有
// syncer 引用并在关闭时调用。幂等：多次调用只生效一次。
func (s *source) Close() error {
	var err error
	s.closeOnce.Do(func() {
		err = s.st.Close()
		if s.syncer != nil {
			s.syncer.Close()
		}
	})
	return err
}

// NewSource 创建真实 binlogsyncer 驱动的事件源。
func NewSource(cfg Config) (binlog.Source, error) {
	flavor := cfg.Flavor
	if flavor == "" {
		flavor = mysql.MySQLFlavor
	}
	sync := replication.BinlogSyncerConfig{
		ServerID:       cfg.ServerID,
		Flavor:         flavor,
		Host:           cfg.Host,
		Port:           uint16(cfg.Port),
		User:           cfg.User,
		Password:       cfg.Password,
		RawModeEnabled: false, // 解析模式；BinlogEvent.RawData 始终可用，归档照常还原
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
		syncer.Close() // StartSync* 的 prepare 阶段可能已建立连接，失败时回收，避免泄漏半开连接
		return nil, fmt.Errorf("stream: start sync: %w", err)
	}
	return &source{st: &binlogStreamer{s: st}, syncer: syncer}, nil
}

func NewSourceWithStreamer(st streamer) binlog.Source {
	return &source{st: st}
}
