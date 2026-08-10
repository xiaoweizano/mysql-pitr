package binlog

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
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

// anonymousTxID 为无 GTID 无 XID 的匿名事务生成确定性 TxID：
// "tx-" + hex(sha256(commitTs 纳秒 + 行签名))。
//
// 同一 binlog 文件重复扫描结果一致（SELECTED_SQL 两阶段定向二次扫描的匹配
// 基础）；NewTransaction 的随机占位 TxID 会在 emit 拿到行数据后被替换掉。
// 注：commitTs 来自事件头（秒级精度），内容与时间戳完全相同的两个匿名事务
// 会得到相同 TxID——这是匿名事务可用的最强判别信息，可接受（报告说明）。
func anonymousTxID(commitTs time.Time, rows []RowChange) string {
	h := sha256.New()
	var ts [8]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(commitTs.UnixNano()))
	h.Write(ts[:])
	h.Write(rowSignature(rows))
	return "tx-" + hex.EncodeToString(h.Sum(nil))
}

// maxSigRows 限制参与行签名的事务行数：大事务（万级行）只取前 maxSigRows 个
// RowChange，控制哈希成本（每值最多写前 64 字节，见 appendImagePrefix）。
// 签名只用于区分事务内容，截断不影响确定性。
const maxSigRows = 256

// rowSignature 累积行变更签名：schema.table + action 字节 + 每行镜像值前缀。
func rowSignature(rows []RowChange) []byte {
	buf := make([]byte, 0, 256)
	n := len(rows)
	if n > maxSigRows {
		n = maxSigRows
	}
	for i := 0; i < n; i++ {
		rc := rows[i]
		buf = append(buf, rc.Schema...)
		buf = append(buf, '.')
		buf = append(buf, rc.Table...)
		buf = append(buf, byte(rc.Action))
		for _, v := range rc.Before {
			buf = appendImagePrefix(buf, v)
		}
		for _, v := range rc.After {
			buf = appendImagePrefix(buf, v)
		}
	}
	return buf
}

// appendImagePrefix 追加镜像值的前 64 字节：[]byte/string 截断（BLOB/TEXT
// 不整值入签名），其余标量（int/float/decimal/time 等）用确定性文本表示。
func appendImagePrefix(buf []byte, v interface{}) []byte {
	switch x := v.(type) {
	case []byte:
		if len(x) > 64 {
			x = x[:64]
		}
		return append(buf, x...)
	case string:
		if len(x) > 64 {
			x = x[:64]
		}
		return append(buf, x...)
	default:
		return fmt.Appendf(buf, "%v", x)
	}
}
