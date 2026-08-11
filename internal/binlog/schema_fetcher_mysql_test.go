//go:build integration

package binlog_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/connector"
)

// TestE2E_FetchSchema 连真 MySQL 验证 connector.MySQLConnector.FetchSchema：
// information_schema 拉取的列元数据（名称/类型/可空/自增）与主键顺序，覆盖
// main 侧 MySQLSchemaFetcher（binlog.SchemaFetcher 的生产实现）对真实实例的
// 路径。worktree 中的占位文件迁入时补全为实际集成测试（worktree 的
// MySQLSchemaFetcher 承诺"Task 6 添加"，实际实现落在 connector 包）。
//
// 前置条件：E2E_MYSQL_DSN（缺则 SKIP）；不需要 E2E_BINLOG_DIR（只读 schema）。
func TestE2E_FetchSchema(t *testing.T) {
	dsn := os.Getenv("E2E_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set E2E_MYSQL_DSN to run integration tests")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()

	// 复合主键 + 可空/不可空/自增列的判别性表结构
	const dbName = "e2e_schema"
	_, err = db.Exec("DROP DATABASE IF EXISTS " + dbName)
	require.NoError(t, err)
	defer db.Exec("DROP DATABASE IF EXISTS " + dbName)
	_, err = db.Exec("CREATE DATABASE " + dbName)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + dbName + `.t (
		part VARCHAR(8)  NOT NULL,
		id   INT         NOT NULL,
		name VARCHAR(32) NULL,
		note TEXT,
		extra INT AUTO_INCREMENT,
		PRIMARY KEY (part, id),
		KEY idx_extra (extra)
	) ENGINE=InnoDB`)
	require.NoError(t, err)

	sch, err := connector.NewMySQLConnectorWithDB(db).
		FetchSchema(context.Background(), dbName, "t")
	require.NoError(t, err, "FetchSchema")

	require.Len(t, sch.Columns, 5, "column count")
	cols := sch.Columns
	assert.Equal(t, "part", cols[0].Name)
	assert.Equal(t, "varchar", cols[0].Type)
	assert.False(t, cols[0].Nullable)
	assert.False(t, cols[0].IsAutoInc)
	assert.Equal(t, "id", cols[1].Name)
	assert.Equal(t, "int", cols[1].Type)
	assert.False(t, cols[1].Nullable)
	assert.Equal(t, "name", cols[2].Name)
	assert.True(t, cols[2].Nullable)
	assert.Equal(t, "note", cols[3].Name)
	assert.Equal(t, "text", cols[3].Type)
	assert.True(t, cols[3].Nullable)
	assert.Equal(t, "extra", cols[4].Name)
	assert.False(t, cols[4].Nullable)
	assert.True(t, cols[4].IsAutoInc, "auto_increment column flagged")

	// 主键按定义顺序（part 先于 id），与表定义一致
	assert.Equal(t, []string{"part", "id"}, sch.PrimaryKey, "primary key order")
}
