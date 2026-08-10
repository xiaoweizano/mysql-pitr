package scan

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/reverse"
)

type Mode int

const (
	ModeMetaOnly    Mode = iota // 仅事务元数据（轻量）
	ModeWithSQL                 // 元数据 + 逆向 SQL（边扫边生成）
	ModeSelectedSQL             // 定向二次扫描：Filter.SelectedTxIDs 命中才生成 SQL
)

type TxMeta struct {
	TxID       string
	GTID       string
	XID        uint64
	CommitTime time.Time
	Schema     string
	Tables     []binlog.TableRef
	RowCount   int
	Truncated  bool
}

type Result struct {
	Meta TxMeta
	SQL  []reverse.Statement // 非 META_ONLY 时填充；SQL=="" 的行表示 warning-only
}

type Config struct {
	ArchiveDir    string
	Filter        binlog.Filter
	Mode          Mode
	MaxPreview    int // 达到即停；默认 500
	SchemaFetcher binlog.SchemaFetcher
	MaxRowsPerTx  int // 0 = 默认 1_000_000
	Logger        *slog.Logger
}

// Stream 跑一次扫描，产出 Result 流与终止错误。调用方必须消费到 channel 关闭。
func Stream(ctx context.Context, cfg Config) (<-chan Result, <-chan error) {
	out := make(chan Result, 16)
	errCh := make(chan error, 1)
	go func() {
		// 子 ctx：MaxPreview 早退 / 提前返回时必须 cancel，否则解析 goroutine
		// 会继续扫完整个归档（大文件白耗 IO，final review Minor）。defer 保证
		// 所有退出路径（MaxPreview、错误、EOF、父 ctx 取消）都取消。
		ctx, cancel := context.WithCancel(ctx)
		defer close(out)
		defer close(errCh)
		if cfg.MaxPreview <= 0 {
			cfg.MaxPreview = 500
		}
		if cfg.Logger == nil {
			cfg.Logger = slog.Default()
		}
		f := cfg.Filter
		f.BinlogDir = cfg.ArchiveDir
		if cfg.MaxRowsPerTx > 0 {
			f.MaxRowsPerTx = cfg.MaxRowsPerTx
		}

		s := binlog.NewScanner(cfg.SchemaFetcher, binlog.WithLogger(cfg.Logger))
		// 两个 defer 都必须注册在任何 return 之前（vet lostcancel）。cancel 声明在
		// s.Close 之后 → LIFO 先执行 cancel 再执行 s.Close：先停解析，再关 scanner。
		defer s.Close()
		defer cancel()
		if err := s.Scan(ctx, f); err != nil {
			errCh <- err
			return
		}

		sent := 0 // 已发送结果数（不能看 len(out)：channel 有缓冲，消费者可能滞后）
		for {
			tx, err := s.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			meta := TxMeta{
				TxID:       tx.TxID,
				GTID:       tx.GTID,
				XID:        tx.XID,
				CommitTime: tx.CommitTime,
				Schema:     tx.Schema,
				RowCount:   tx.RowCount(),
				Truncated:  tx.Truncated,
			}
			for _, rc := range tx.Statements {
				ref := binlog.TableRef{Schema: rc.Schema, Table: rc.Table}
				if !containsRef(meta.Tables, ref) {
					meta.Tables = append(meta.Tables, ref)
				}
			}
			r := Result{Meta: meta}
			if cfg.Mode != ModeMetaOnly {
				r.SQL = generateSQL(cfg.SchemaFetcher, tx)
			}
			select {
			case out <- r:
				sent++
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			if sent >= cfg.MaxPreview {
				return
			}
		}
	}()
	return out, errCh
}

// containsRef 线性查找 tables 中是否已含 ref；已含则返回 true（用于去重）。
func containsRef(tables []binlog.TableRef, ref binlog.TableRef) bool {
	for _, t := range tables {
		if t == ref {
			return true
		}
	}
	return false
}

// generateSQL 把一个事务翻成逆向 SQL 列表；Truncated 或生成失败时返回
// warning-only 语句（SQL==""）。
func generateSQL(sf binlog.SchemaFetcher, tx *binlog.Transaction) []reverse.Statement {
	if tx.Truncated {
		return []reverse.Statement{{
			SQL: "", TxID: tx.TxID,
			Warnings: []string{"transaction truncated, cannot generate full reverse SQL"},
		}}
	}
	ctx := context.Background()
	schema := map[string]binlog.TableSchema{}
	for _, rc := range tx.Statements {
		key := rc.Schema + "." + rc.Table
		if _, ok := schema[key]; ok {
			continue
		}
		sch, err := sf.FetchSchema(ctx, rc.Schema, rc.Table)
		if err != nil {
			continue // reverse.Generate 对缺表输出 warning
		}
		schema[key] = sch
	}
	stmts, err := reverse.Generate(tx, schema, reverse.Options{})
	if err != nil {
		return []reverse.Statement{{
			SQL: "", TxID: tx.TxID,
			Warnings: []string{"reverse generate failed: " + err.Error()},
		}}
	}
	return stmts
}
