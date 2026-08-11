//go:build integration

package collector_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/archive"
	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/collector"
	"github.com/a-shan/mysql-pitr/internal/config"
	"github.com/a-shan/mysql-pitr/internal/connector"
	"github.com/a-shan/mysql-pitr/internal/stream"
)

// 归档循环真实 e2e：连真 MySQL → 跑 DML（CREATE TABLE + INSERT/UPDATE/DELETE，
// 3 个事务，前后采集 GTID 差集）→ 启动 Loop.Run（真实 connector + 真实
// stream.NewSource 包装）→ 确认回填/续拉 → FLUSH LOGS 触发轮转 → 停止 →
// 断言归档目录完整、archive_state.json 存在、binlog.Scanner 扫归档能还原 DML
// 事务（与直接扫 E2E_BINLOG_DIR 结果对比一致）。
//
// 前置条件（不满足则 SKIP，与 internal/binlog/e2e_test.go 同模式）：
//   - E2E_MYSQL_DSN：go-sql-driver DSN，必须 tcp 且带 binlog 复制权限
//     （REPLICATION SLAVE / REPLICATION CLIENT / SELECT）
//   - E2E_BINLOG_DIR：mysqld 的 binlog 目录（datadir），测试进程须可读
//   - 实例必须 GTID（gtid_mode=ON）与 binlog_format=ROW
//   - 建议专用/新起的测试实例：reconcile 会把 SHOW BINARY LOGS 的全部文件
//     回填进临时归档目录，历史 binlog 越多越慢
//
// 时序要点（无 sleep，靠文件/状态轮询，确定性强）：
//   - 被断言的事务先于 Run 提交：它们落在「当前打开文件 F1」里，reconcile 把
//     F1 回填为前缀（magic+FDE+DDL+DML），保证续拉路径不依赖流式时序。
//   - 心跳 INSERT 在 F1 回填完成、Loop 已接流后发出，直到 F1.partial 出现
//     （append 已写内容，loop 处于续拉态）才停 → 才 FLUSH。这避免「空文件在
//     reconcile 时被回填成 4 字节 magic 前缀、续拉尾部又丢弃 FDE」的退化情形
//     （见 task-10-report 顾虑 1）。
//   - FLUSH LOGS 后轮询 archive_state.json（轮转封口 + SaveState 完成的标志）。
func TestE2E_ArchiveLoop(t *testing.T) {
	dsn := os.Getenv("E2E_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set E2E_MYSQL_DSN to run integration tests")
	}
	binlogDir := os.Getenv("E2E_BINLOG_DIR")
	if binlogDir == "" {
		t.Skip("set E2E_BINLOG_DIR to run integration tests")
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	if reason, ok := e2eInstanceOK(t, db); !ok {
		t.Skipf("E2E instance not suitable: %s", reason)
	}

	// 独立库名，避免与 binlog e2e 的 e2e.* 互相干扰
	const dbName = "e2e_collector"
	execSQL(t, db, "DROP DATABASE IF EXISTS "+dbName)
	defer execSQL(t, db, "DROP DATABASE IF EXISTS "+dbName)
	execSQL(t, db, "CREATE DATABASE "+dbName)
	// 表 DDL 是 QueryEvent（非行事务，不会被 GTID 过滤命中），F1 因此非空，
	// reconcile 回填前缀会包含 FDE（退化情形规避的前提）
	execSQL(t, db, "CREATE TABLE "+dbName+".t (id INT PRIMARY KEY, v VARCHAR(32))")

	// 被断言的 DML 集合：3 个 autocommit 事务
	gtidBefore := e2eCaptureGTID(t, db)
	execSQL(t, db, "INSERT INTO "+dbName+".t VALUES (1,'a'),(2,'b')")
	execSQL(t, db, "UPDATE "+dbName+".t SET v='x' WHERE id=1")
	execSQL(t, db, "DELETE FROM "+dbName+".t WHERE id=2")
	gtidAfter := e2eCaptureGTID(t, db)
	added := e2eSubtractGTID(gtidAfter, gtidBefore)
	require.False(t, added.IsEmpty(), "DML produced no GTIDs")

	// 归档循环接线：真实 connector（MySQLInfo + SchemaFetcher）、真实
	// DefaultSourceFactory（wrap stream.NewSource）、固定 ServerID
	conn := connector.NewMySQLConnectorWithDB(db)
	cc, err := config.ParseDSNToConnConfig(dsn)
	require.NoError(t, err, "parse DSN to ConnConfig")
	scfg := stream.Config{
		Host:     cc.Host,
		Port:     cc.Port,
		User:     cc.User,
		Password: cc.Password,
		ServerID: e2eServerID,
	}
	archiveDir := t.TempDir()
	cfg := collector.Config{
		MySQL:         conn,
		BinlogDir:     binlogDir,
		ArchiveDir:    archiveDir,
		ServerID:      e2eServerID,
		SourceFactory: collector.DefaultSourceFactory(scfg),
	}
	loop := collector.NewLoop(cfg, archive.NewWriter(archiveDir))

	// Run 前拍下文件清单；reconcile 会把它们全部回填（含当前打开文件的前缀）
	preFiles := showBinlogs(t, conn)
	require.NotEmpty(t, preFiles, "expect at least the current binlog file")
	openName := preFiles[len(preFiles)-1]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- loop.Run(ctx) }()

	// 1) 等待回填完成：SHOW BINARY LOGS 的每个文件都出现在归档目录
	waitLoop(t, 60*time.Second, errCh, "reconcile backfill of all binlog files", func() bool {
		for _, f := range preFiles {
			if !fileExists(archiveDir, f) {
				return false
			}
		}
		return true
	})

	// 2) 心跳 INSERT 直到 loop 接流（当前打开文件的 .partial 出现 = append 已写内容）
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		for i := 0; ; i++ {
			select {
			case <-hbCtx.Done():
				return
			default:
			}
			if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s.t VALUES (%d, 'hb')", dbName, 1000+i)); err != nil {
				return // 尽力而为；liveness 由 poll 判定
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()
	waitLoop(t, 60*time.Second, errCh, "loop to begin appending (live stream)", func() bool {
		return fileExists(archiveDir, openName+".partial")
	})
	hbCancel()
	<-hbDone

	// 3) FLUSH LOGS 触发轮转 → 封口 F1（append 组合验证）+ SaveState
	execSQL(t, db, "FLUSH LOGS")
	waitLoop(t, 60*time.Second, errCh, "rotation seal + archive_state.json", func() bool {
		return fileExists(archiveDir, "archive_state.json")
	})

	// 4) 停止：Run 须以 nil 返回（ctx 取消的干净停止）
	cancel()
	err = <-errCh
	require.NoError(t, err, "loop.Run must return nil on ctx cancel")

	// 断言
	finalFiles := showBinlogs(t, conn)
	require.NotEmpty(t, finalFiles, "expect at least one binlog file")

	// 4a. archive_state.json 内容：LastFile 指向 FLUSH 后的当前打开文件，LastPos=0
	st, err := collector.LoadState(archiveDir)
	require.NoError(t, err, "load archive_state.json")
	assert.NotEmpty(t, st.LastFile, "state.LastFile")
	assert.Zero(t, st.LastPos, "state.LastPos (rotated to a fresh file)")
	assert.Equal(t, finalFiles[len(finalFiles)-1], st.LastFile, "state points to the current open file")

	// 4b. 除当前打开文件外，全部 SHOW BINARY LOGS 文件已封口归档
	for _, f := range finalFiles[:len(finalFiles)-1] {
		assert.FileExists(t, filepath.Join(archiveDir, f), "archived sealed file %s", f)
	}

	// 4c. 封口的 F1（含 DML 的文件）完整：归档大小≈服务器大小（±512，容忍
	// 版本间 rotate/FDE 落点的差异；精确一致性由 4e 的扫描内容断言）
	assert.InDelta(t, fileSize(t, binlogDir, openName), fileSize(t, archiveDir, openName), 512,
		"sealed archive file %s size matches server", openName)

	// 4d. 无残留 .partial（Run 返回时 loop 已清理）
	entries, err := os.ReadDir(archiveDir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".partial", "no stray partial after clean stop")
	}

	// 4e. 扫归档能还原 3 个 DML 事务，且与直扫 E2E_BINLOG_DIR 一致
	fromArchive := scanTXSummaries(t, archiveDir, added, conn)
	fromServer := scanTXSummaries(t, binlogDir, added, conn)
	require.Len(t, fromArchive, 3, "archive scan must cover the 3 DML transactions")
	require.Len(t, fromServer, 3, "server scan must cover the 3 DML transactions")
	serverByGTID := make(map[string]int, len(fromServer))
	for _, s := range fromServer {
		serverByGTID[s.GTID] = s.Rows
	}
	for _, s := range fromArchive {
		want, ok := serverByGTID[s.GTID]
		require.True(t, ok, "archive tx %s not present in server scan", s.GTID)
		assert.Equal(t, want, s.Rows, "row count for tx %s (archive vs server)", s.GTID)
	}
	assert.Equal(t, added.String(), gtidSetString(t, fromArchive), "archive tx GTID set == captured DML GTID set")
}

// e2eServerID 是归档 syncer 的固定 server-id。测试实例上应唯一（README 提示）。
const e2eServerID = 424242

// txSummary 是扫描命中的单个事务的判别性摘要。
type txSummary struct {
	GTID string
	Rows int
}

// scanTXSummaries 用 GTID 过滤扫 dir 下的 binlog，返回命中事务的 GTID/行数。
func scanTXSummaries(t *testing.T, dir string, added *mysql.MysqlGTIDSet, sf binlog.SchemaFetcher) []txSummary {
	t.Helper()
	sc := binlog.NewScanner(sf)
	require.NoError(t, sc.Scan(context.Background(), binlog.Filter{BinlogDir: dir, GTIDSet: added}),
		"scan %s", dir)
	var out []txSummary
	for {
		tx, err := sc.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "scanner.Next in %s", dir)
		require.NotEmpty(t, tx.GTID, "tx without GTID under GTID filter in %s", dir)
		out = append(out, txSummary{GTID: tx.GTID, Rows: len(tx.Statements)})
	}
	return out
}

// gtidSetString 把扫描命中的 GTID 列表合成为一个 GTID 集字符串（与
// @@global.gtid_executed 的渲染一致，可和 added.String() 直接比较）。
func gtidSetString(t *testing.T, txs []txSummary) string {
	t.Helper()
	gtids := make([]string, 0, len(txs))
	for _, s := range txs {
		gtids = append(gtids, s.GTID)
	}
	if len(gtids) == 0 {
		return ""
	}
	s, err := binlog.ParseGTIDSet("mysql", strings.Join(gtids, ","))
	require.NoError(t, err, "re-assemble scanned GTID set")
	return s.String()
}

// showBinlogs 返回当前 SHOW BINARY LOGS 的文件名列表（经 connector 适配，
// 兼容 MySQL 8.0.14+ 的 Encrypted 列）。
func showBinlogs(t *testing.T, conn *connector.MySQLConnector) []string {
	t.Helper()
	files, err := conn.ListBinlogs(context.Background())
	require.NoError(t, err, "SHOW BINARY LOGS")
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.Name)
	}
	return out
}

// e2eInstanceOK 检查实例是否满足 e2e 前置条件（GTID ON + ROW binlog）。
func e2eInstanceOK(t *testing.T, db *sql.DB) (string, bool) {
	t.Helper()
	var gtidMode, binlogFormat string
	if err := db.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		return "cannot query @@gtid_mode: " + err.Error(), false
	}
	if err := db.QueryRow("SELECT @@binlog_format").Scan(&binlogFormat); err != nil {
		return "cannot query @@binlog_format: " + err.Error(), false
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return "gtid_mode is " + gtidMode + " (want ON)", false
	}
	if !strings.EqualFold(binlogFormat, "ROW") {
		return "binlog_format is " + binlogFormat + " (want ROW)", false
	}
	return "", true
}

// e2eCaptureGTID 读取当前 @@global.gtid_executed。
func e2eCaptureGTID(t *testing.T, db *sql.DB) *mysql.MysqlGTIDSet {
	t.Helper()
	var raw string
	require.NoError(t, db.QueryRow("SELECT @@global.gtid_executed").Scan(&raw))
	if strings.TrimSpace(raw) == "" {
		s := mysql.NewMysqlGTIDSet()
		return &s
	}
	s, err := binlog.ParseGTIDSet("mysql", raw)
	require.NoError(t, err, "parse gtid_executed %q", raw)
	return s.(*mysql.MysqlGTIDSet)
}

// e2eSubtractGTID 返回 after 中不在 before 里的 GTID（action 新增的 GTID）。
// go-mysql v1.16 的 GTIDSet 接口没有 Minus，按区间粒度做差集（见
// internal/binlog/e2e_test.go 的 subtractGTIDSets，逻辑一致）。
func e2eSubtractGTID(after, before *mysql.MysqlGTIDSet) *mysql.MysqlGTIDSet {
	diff := mysql.NewMysqlGTIDSet()
	for sid, tags := range *after {
		for tag, ivs := range tags {
			beforeIvs := (*before)[sid][tag]
			for _, iv := range ivs {
				for _, rem := range subtractIntervals(iv, beforeIvs) {
					for gno := rem.Start; gno < rem.Stop; gno++ {
						diff.AddGTIDWithTag(sid, tag, gno)
					}
				}
			}
		}
	}
	return &diff
}

// subtractIntervals 从 iv 挖掉 before 的区间（before 须归一化且升序），
// 返回剩余区间列表（均为 [Start, Stop) 半开区间）。
func subtractIntervals(iv mysql.Interval, before []mysql.Interval) []mysql.Interval {
	var out []mysql.Interval
	cur := iv.Start
	for _, b := range before {
		if b.Stop <= cur {
			continue
		}
		if b.Start >= iv.Stop {
			break
		}
		if b.Start > cur {
			out = append(out, mysql.Interval{Start: cur, Stop: b.Start})
		}
		if b.Stop >= iv.Stop {
			cur = iv.Stop
			break
		}
		cur = b.Stop
	}
	if cur < iv.Stop {
		out = append(out, mysql.Interval{Start: cur, Stop: iv.Stop})
	}
	return out
}

func execSQL(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	_, err := db.Exec(q)
	require.NoError(t, err, "exec %q", q)
}

func fileExists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

func fileSize(t *testing.T, dir, name string) float64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(dir, name))
	require.NoError(t, err, "stat %s in %s", name, dir)
	return float64(info.Size())
}

// waitLoop 轮询 cond；loop.Run 报错或超时则失败并给出 loop 的错误。
func waitLoop(t *testing.T, timeout time.Duration, errCh <-chan error, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("loop.Run failed before %s: %v", what, err)
		default:
		}
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case err := <-errCh:
		t.Fatalf("loop.Run failed before %s: %v", what, err)
	default:
	}
	t.Fatalf("timeout waiting for %s", what)
}
