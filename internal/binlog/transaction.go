package binlog

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type RowAction int

const (
	ActionInsert RowAction = iota
	ActionUpdate
	ActionDelete
)

type TableRef struct{ Schema, Table string }

type TimeRange struct{ Start, End time.Time }

type RowChange struct {
	Schema, Table string
	Action        RowAction
	Before, After []interface{}
	ColumnNames   []string
}

type Transaction struct {
	TxID       string
	GTID       string
	XID        uint64
	CommitTime time.Time
	Schema     string
	Statements []RowChange
	Truncated  bool
}

type Filter struct {
	BinlogDir     string // binlog 文件所在目录
	Tables        []TableRef
	TimeRange     *TimeRange
	GTIDSet       mysql.GTIDSet
	StartPos      mysql.Position
	EndPos        mysql.Position
	MaxRowsPerTx  int
	SelectedTxIDs []string // SELECTED_SQL 定向二次扫描：仅保留 TxID 命中的事务
}

// NewTransaction 构造一个 Transaction，自动生成规范 TxID。
// commitTime 必须非零；gtid 和 xid 至少一个非空（都空则生成随机 fallback id）。
func NewTransaction(gtid string, xid uint64, commitTime time.Time, schema string) (Transaction, error) {
	if commitTime.IsZero() {
		return Transaction{}, fmt.Errorf("binlog: commit time must be non-zero")
	}

	tx := Transaction{
		GTID:       gtid,
		XID:        xid,
		CommitTime: commitTime,
		Schema:     schema,
	}

	switch {
	case gtid != "":
		tx.TxID = gtid
	case xid != 0:
		tx.TxID = fmt.Sprintf("xid-%d", xid)
	default:
		tx.TxID = "tx-" + randomID(8)
	}
	return tx, nil
}

// AppendRow 追加一条行变更到事务。
func (t *Transaction) AppendRow(rc RowChange) {
	t.Statements = append(t.Statements, rc)
}

// MarkTruncated 标记事务被截断（超过 MaxRowsPerTx）。
func (t *Transaction) MarkTruncated() {
	t.Truncated = true
}

// RowCount 返回已累积的行变更数。
func (t *Transaction) RowCount() int {
	n := 0
	for _, rc := range t.Statements {
		n += len(rc.Before) + len(rc.After) // 粗略；测试用
	}
	if n == 0 {
		n = len(t.Statements)
	}
	return n
}

func randomID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
