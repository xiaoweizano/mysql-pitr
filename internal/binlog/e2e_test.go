//go:build integration

package binlog_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/reverse"
)

// 端到端 8 场景测试矩阵：setup SQL → 执行 action → 扫描 binlog →
// reverse.Generate 生成逆向 SQL → executor.Run 执行 → 断言数据库最终状态。
//
// 前置条件（环境变量 + 实例要求，不满足则 SKIP）：
//   - E2E_MYSQL_DSN：go-sql-driver DSN，如 "root:@tcp(127.0.0.1:33068)/"
//   - E2E_BINLOG_DIR：mysqld 的 binlog 目录（datadir）
//   - 实例必须开启 GTID（gtid_mode=ON）与 binlog_format=ROW
//   - 测试进程需能直接读取 E2E_BINLOG_DIR 下的 binlog 文件（本机无 docker，
//     由用户在 docker-capable 主机/容器内执行，见 scripts/e2e/README.md）
//
// 过滤策略：不用骨架里的 TimeRange（binlog 事件时间戳是秒级精度，setup 与
// action 落在同一秒内会连 setup 事务一起扫到，不可判定），而是对每个场景在
// action 前后各读一次 @@global.gtid_executed，取差集作为 Filter.GTIDSet，
// 精确命中 action 产生的事务。实例的 GTID 集跨测试累积，因此每次都在 action
// 前后紧邻采集。
//
// 迁入说明（2026-08-11，worktree → main）：当前 main 的 binlog.Filter 增了
// SelectedTxIDs/EndPos、reverse 的主键定位/值格式化、Transaction.TxID 确定性，
// 本测试与之兼容——唯一适配是 mysqlSchemaFetcher 现在同时拉取主键（reverse 走
// 主键精确定位路径，与 connector.FetchSchema 行为一致）；Filter.GTIDSet 仍用
// *mysql.MysqlGTIDSet（实现 mysql.GTIDSet 接口），executor API 未变。

func TestE2E_SimpleDeleteRollback(t *testing.T) {
	runE2E(t, e2eScenario{
		name: "simple_delete_rollback",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
			"CREATE TABLE e2e.t (id INT PRIMARY KEY, name VARCHAR(32));",
			"INSERT INTO e2e.t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
		},
		action: "DELETE FROM e2e.t WHERE id = 2;",
		wantTx: 1,
		// 逆向 INSERT 应恢复被删行 → 回滚后 3 行
		assertQuery:    "SELECT COUNT(*) FROM e2e.t",
		assertExpected: "3",
	})
}

func TestE2E_SimpleUpdateRollback(t *testing.T) {
	runE2E(t, e2eScenario{
		name: "simple_update_rollback",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
			"CREATE TABLE e2e.t (id INT PRIMARY KEY, v INT);",
			"INSERT INTO e2e.t VALUES (1, 100);",
		},
		action: "UPDATE e2e.t SET v = 200 WHERE id = 1;",
		wantTx: 1,
		// 逆向 UPDATE 应把 v 从 200 改回 100
		assertQuery:    "SELECT v FROM e2e.t WHERE id = 1",
		assertExpected: "100",
	})
}

func TestE2E_SimpleInsertRollback(t *testing.T) {
	runE2E(t, e2eScenario{
		name: "simple_insert_rollback",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
			"CREATE TABLE e2e.t (id INT PRIMARY KEY);",
		},
		action: "INSERT INTO e2e.t VALUES (99);",
		wantTx: 1,
		// 逆向 DELETE 应移除插入的行 → 回滚后 0 行
		assertQuery:    "SELECT COUNT(*) FROM e2e.t",
		assertExpected: "0",
	})
}

func TestE2E_LargeTransaction(t *testing.T) {
	// 10 万行 setup（100 × 1000 行 multi-values INSERT，属 setup 不参与扫描），
	// action 在一个事务里 DELETE 5 万行 → reverse 重新生成 5 万条单行 INSERT
	// （每条远小于默认 16 KiB MaxStatementSize），executor 以 BatchSize 10
	// 分 5000 批执行。耗时较长属预期。
	setup := []string{
		"DROP DATABASE IF EXISTS e2e;",
		"CREATE DATABASE e2e;",
		"CREATE TABLE e2e.t (id INT PRIMARY KEY);",
	}
	for i := 0; i < 100; i++ {
		setup = append(setup, fmt.Sprintf("INSERT INTO e2e.t VALUES %s;", multiValues(1000, i*1000)))
	}
	runE2E(t, e2eScenario{
		name:   "large_tx_rollback",
		setup:  setup,
		action: "DELETE FROM e2e.t WHERE id < 50000;",
		wantTx: 1,
		// 5 万行全部恢复 → 回到 10 万行
		assertQuery:    "SELECT COUNT(*) FROM e2e.t",
		assertExpected: "100000",
	})
}

func TestE2E_MixedDDLAndDML(t *testing.T) {
	// action 内含 CREATE TABLE（DDL 不可逆、不产生行事务，扫描时应被忽略）
	// 与 INSERT（应被回滚）。
	runE2E(t, e2eScenario{
		name: "mixed_ddl_dml",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
		},
		action: []string{
			"CREATE TABLE e2e.t (id INT PRIMARY KEY);",
			"INSERT INTO e2e.t VALUES (1), (2);",
		},
		wantTx:         1, // DDL 无行变更不输出；只剩 INSERT 事务
		assertQuery:    "SELECT COUNT(*) FROM e2e.t",
		assertExpected: "0",
	})
}

func TestE2E_CrossBinlogFileTransaction(t *testing.T) {
	// FLUSH LOGS 把 binlog 切换到下一个文件；被回滚的事务（DELETE id=2，
	// 目标行是切换后插入的）落在新文件里，扫描必须跨文件找到它。
	runE2E(t, e2eScenario{
		name: "cross_binlog",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
			"CREATE TABLE e2e.t (id INT PRIMARY KEY);",
			"INSERT INTO e2e.t VALUES (1);",
			"FLUSH LOGS;",
			"INSERT INTO e2e.t VALUES (2);",
		},
		action: "DELETE FROM e2e.t WHERE id = 2;",
		wantTx: 1,
		// 逆向 INSERT 恢复 id=2 → 回到 2 行
		assertQuery:    "SELECT COUNT(*) FROM e2e.t",
		assertExpected: "2",
	})
}

func TestE2E_UserCancelsMidExecution(t *testing.T) {
	// action 是 5 条独立 autocommit INSERT → 5 个事务 → 逆向得到 5 条 DELETE。
	// BatchSize=1 时每条 DELETE 是独立批次；取消通过 ProgressCallback 实现：
	// 第 2 批提交后（p.Done == 2）取消 ctx → 执行器恰好完成 2 批即暂停，
	// 3 行保留。基于回调计数，无 sleep，完全确定。
	runE2E(t, e2eScenario{
		name: "user_cancel",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
			"CREATE TABLE e2e.t (id INT PRIMARY KEY);",
		},
		action: []string{
			"INSERT INTO e2e.t VALUES (1);",
			"INSERT INTO e2e.t VALUES (2);",
			"INSERT INTO e2e.t VALUES (3);",
			"INSERT INTO e2e.t VALUES (4);",
			"INSERT INTO e2e.t VALUES (5);",
		},
		batchSize:   1,
		cancelAfter: 2,
		wantTx:      5,
		// 恰好 2 批（id=1、2）被删，id=3、4、5 保留 → 3 行
		assertQuery:    "SELECT COUNT(*) FROM e2e.t",
		assertExpected: "3",
	})
}

func TestE2E_GTIDPositioning(t *testing.T) {
	// 捕获 action 前后 @@global.gtid_executed 的差集，构造只含该 GTID 的
	// Filter.GTIDSet → 扫描必须恰好命中 1 个事务，且该事务携带的 GTID 与
	// 差集完全一致；回滚后行数还原。
	runE2E(t, e2eScenario{
		name: "gtid_positioning",
		setup: []string{
			"DROP DATABASE IF EXISTS e2e;",
			"CREATE DATABASE e2e;",
			"CREATE TABLE e2e.t (id INT PRIMARY KEY);",
		},
		action:          "INSERT INTO e2e.t VALUES (42);",
		wantTx:          1,
		assertExactGTID: true,
		assertQuery:     "SELECT COUNT(*) FROM e2e.t",
		assertExpected:  "0",
	})
}

// e2eScenario 描述一个端到端测试场景。
type e2eScenario struct {
	name            string
	setup           []string
	action          interface{} // string 或 []string
	batchSize       int         // executor 批次大小；0 = 默认 10
	cancelAfter     int         // >0：执行完 N 个批次后取消（ProgressCallback 计数，确定性）
	wantTx          int         // 期望扫描到的事务数；0 = 不校验
	assertExactGTID bool        // 断言扫描命中的唯一事务的 GTID 与捕获差集完全一致
	assertQuery     string
	assertExpected  string
}

func runE2E(t *testing.T, s e2eScenario) {
	t.Helper()
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

	if reason, ok := instanceOK(t, db); !ok {
		t.Skipf("E2E instance not suitable: %s", reason)
	}

	// 1. setup
	for _, q := range s.setup {
		_, err := db.Exec(q)
		require.NoError(t, err, "setup: %s", q)
	}

	// 2. action — 前后紧邻采集 GTID，差集即 action 产生的事务
	gtidBefore := captureGTIDSet(t, db)
	execAction(t, db, s.action)
	gtidAfter := captureGTIDSet(t, db)
	added := subtractGTIDSets(gtidAfter, gtidBefore)
	require.False(t, added.IsEmpty(), "action produced no GTIDs")

	// 3. 扫描 binlog：per-transaction GTID 精确过滤
	sc := binlog.NewScanner(mysqlSchemaFetcher{db: db})
	filter := binlog.Filter{
		BinlogDir: binlogDir,
		GTIDSet:   added,
		Tables:    []binlog.TableRef{{Schema: "e2e", Table: "t"}},
	}
	require.NoError(t, sc.Scan(context.Background(), filter))

	// 4. 收集事务 + 生成逆向 SQL（schema 实时取自 information_schema）
	schema, err := (mysqlSchemaFetcher{db: db}).FetchSchema(context.Background(), "e2e", "t")
	require.NoError(t, err, "fetch schema for e2e.t")
	schemaMap := map[string]binlog.TableSchema{"e2e.t": schema}

	var txs []*binlog.Transaction
	var stmts []reverse.Statement
	for {
		tx, err := sc.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "scanner.Next")
		require.False(t, tx.Truncated, "tx %s unexpectedly truncated", tx.TxID)
		require.NotEmpty(t, tx.GTID, "tx %s has no GTID (GTID filter relies on it)", tx.TxID)
		txs = append(txs, tx)
		rev, err := reverse.Generate(tx, schemaMap, reverse.Options{})
		require.NoError(t, err, "reverse.Generate for tx %s", tx.TxID)
		for _, st := range rev {
			require.NotEmpty(t, st.SQL, "reverse emitted warning statement for tx %s: %v", st.TxID, st.Warnings)
		}
		stmts = append(stmts, rev...)
	}
	if s.wantTx > 0 {
		assert.Equal(t, s.wantTx, len(txs), "scanned transaction count (%s)", s.name)
	}
	if s.assertExactGTID {
		require.Len(t, txs, 1, "exact-GTID assertion needs exactly one scanned tx (%s)", s.name)
		assert.Equal(t, added.String(), txs[0].GTID, "scanned tx GTID vs captured GTID delta (%s)", s.name)
	}
	require.NotEmpty(t, stmts, "no reverse statements for %s", s.name)

	// 5. 执行逆向计划
	batchSize := s.batchSize
	if batchSize == 0 {
		batchSize = 10
	}
	plan := executor.Plan{
		OperationID: "e2e-" + s.name,
		Statements:  stmts,
		DSN:         dsn,
		BatchSize:   batchSize,
	}
	dbFactory := func(executor.Plan) (executor.DB, error) { return &sqlDBAdapter{db: db}, nil }
	store := executor.NewInMemoryCheckpointStore()
	ex := executor.NewExecutor(dbFactory, store)

	ctx := context.Background()
	var cancel context.CancelFunc
	var cb executor.ProgressCallback
	if s.cancelAfter > 0 {
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		cb = func(p executor.Progress) {
			if p.Done >= s.cancelAfter {
				cancel()
			}
		}
	}
	report, err := ex.Run(ctx, plan, cb)
	require.NoError(t, err, "executor.Run (%s)", s.name)
	if s.cancelAfter > 0 {
		assert.True(t, report.Paused, "cancel scenario must pause, not complete (%s)", s.name)
		assert.Equal(t, s.cancelAfter, report.Done, "batches committed before cancel (%s)", s.name)
	} else {
		assert.False(t, report.Paused, "non-cancel scenario must complete (%s)", s.name)
		assert.Equal(t, len(stmts), report.Done, "all statements executed (%s)", s.name)
	}

	// 6. 断言最终数据库状态
	if s.assertQuery != "" {
		got := queryValue(t, db, s.assertQuery)
		assert.Equal(t, s.assertExpected, got, "DB state after rollback (%s)", s.name)
	}
}

func execAction(t *testing.T, db *sql.DB, action interface{}) {
	t.Helper()
	switch a := action.(type) {
	case string:
		_, err := db.Exec(a)
		require.NoError(t, err, "action: %s", a)
	case []string:
		for _, q := range a {
			_, err := db.Exec(q)
			require.NoError(t, err, "action: %s", q)
		}
	default:
		t.Fatalf("unsupported action type %T", action)
	}
}

// instanceOK 检查实例是否满足 e2e 前置条件（GTID ON + ROW binlog）。
func instanceOK(t *testing.T, db *sql.DB) (string, bool) {
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

// captureGTIDSet 读取当前 @@global.gtid_executed。
func captureGTIDSet(t *testing.T, db *sql.DB) *mysql.MysqlGTIDSet {
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

// subtractGTIDSets 返回 after 中不在 before 里的 GTID（action 新增的 GTID）。
// go-mysql v1.16 的 GTIDSet 接口没有 Minus（worktree 时代的 API 有），按
// map[uuid]map[tag]IntervalSlice 结构在区间粒度上做差集（Interval 为
// [Start, Stop) 半开区间，AddGTIDWithTag 逐 gno 补回）。
func subtractGTIDSets(after, before *mysql.MysqlGTIDSet) *mysql.MysqlGTIDSet {
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

// queryValue 执行单值查询并转成字符串，兼容 int64/string/[]byte 等 driver 类型。
func queryValue(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	var raw interface{}
	require.NoError(t, db.QueryRow(q).Scan(&raw))
	switch v := raw.(type) {
	case nil:
		return ""
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// multiValues 生成 "(offset),(offset+1),...,(offset+n-1)" — 批量 INSERT 用。
func multiValues(n, offset int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("(%d)", offset+i)
	}
	return strings.Join(parts, ",")
}

// mysqlSchemaFetcher 通过 information_schema 实时拉取表结构（列 + 主键），
// 实现 binlog.SchemaFetcher。与 connector.MySQLConnector.FetchSchema 的行为
// 一致（含 PrimaryKey 填充，reverse 据此走主键精确定位路径）——迁移时补充了
// 主键查询，使本测试贴合当前 main 的主键感知 reverse。
type mysqlSchemaFetcher struct{ db *sql.DB }

func (m mysqlSchemaFetcher) FetchSchema(ctx context.Context, schema, table string) (binlog.TableSchema, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT COLUMN_NAME, DATA_TYPE,
		       CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END,
		       CASE WHEN EXTRA LIKE '%auto_increment%' THEN 1 ELSE 0 END
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, schema, table)
	if err != nil {
		return binlog.TableSchema{}, fmt.Errorf("query schema: %w", err)
	}
	defer rows.Close()
	var cols []binlog.ColumnDef
	for rows.Next() {
		var c binlog.ColumnDef
		// IS_NULLABLE / EXTRA 是表达式列，二进制协议下返回 int64（Task 14 教训），
		// 必须扫入 int64 而非 bool。
		var nullable, autoInc int64
		if err := rows.Scan(&c.Name, &c.Type, &nullable, &autoInc); err != nil {
			return binlog.TableSchema{}, err
		}
		c.Nullable = nullable == 1
		c.IsAutoInc = autoInc == 1
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return binlog.TableSchema{}, err
	}
	if len(cols) == 0 {
		return binlog.TableSchema{}, fmt.Errorf("table %s.%s not found", schema, table)
	}
	pk, err := m.fetchPrimaryKey(ctx, schema, table)
	if err != nil {
		return binlog.TableSchema{}, err
	}
	return binlog.TableSchema{Schema: schema, Table: table, Columns: cols, PrimaryKey: pk}, nil
}

// fetchPrimaryKey 按主键定义顺序拉取主键列名（KEY_COLUMN_USAGE，与 connector
// 实现一致）。
func (m mysqlSchemaFetcher) fetchPrimaryKey(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION
	`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query primary key: %w", err)
	}
	defer rows.Close()
	var pk []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pk = append(pk, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pk, nil
}

// sqlDBAdapter 把 *sql.DB 适配为 executor.DB。
type sqlDBAdapter struct{ db *sql.DB }

func (a *sqlDBAdapter) Exec(q string, args ...interface{}) (executor.Result, error) {
	res, err := a.db.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	return &sqlResAdapter{res: res}, nil
}

func (a *sqlDBAdapter) Begin() (executor.Tx, error) {
	tx, err := a.db.Begin()
	if err != nil {
		return nil, err
	}
	return &sqlTxAdapter{tx: tx}, nil
}

func (a *sqlDBAdapter) Close() error { return nil }

type sqlResAdapter struct{ res sql.Result }

func (a *sqlResAdapter) LastInsertId() (int64, error) { return a.res.LastInsertId() }
func (a *sqlResAdapter) RowsAffected() (int64, error) { return a.res.RowsAffected() }

type sqlTxAdapter struct{ tx *sql.Tx }

func (a *sqlTxAdapter) Exec(q string, args ...interface{}) (executor.Result, error) {
	res, err := a.tx.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	return &sqlResAdapter{res: res}, nil
}

func (a *sqlTxAdapter) Commit() error   { return a.tx.Commit() }
func (a *sqlTxAdapter) Rollback() error { return a.tx.Rollback() }
