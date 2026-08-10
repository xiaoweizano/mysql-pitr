package reverse

import (
	"github.com/a-shan/mysql-pitr/internal/binlog"
)

// Options 控制 Generate 的行为。
type Options struct {
	IgnoreAutoIncrement bool // INSERT 回滚（即 DELETE）无关；保留为未来扩展
	MaxStatementSize    int  // 单条 SQL 字节数上限；0 = 默认 16 KiB
}

// Statement 是 Generate 输出的单条逆向 SQL。
type Statement struct {
	SQL       string
	TxID      string // 必填，来自 Transaction.TxID
	TxOrder   int    // 同事务内序号（0-based）
	SourceRow binlog.RowChange
	Warnings  []string
}

// DefaultMaxStatementSize 是 MaxStatementSize=0 时使用的默认值。
const DefaultMaxStatementSize = 16 * 1024
