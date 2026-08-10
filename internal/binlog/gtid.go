package binlog

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ParseGTIDSet 解析 GTID 集字符串。flavor 为 "mysql" 或 "mariadb"。
func ParseGTIDSet(flavor, raw string) (mysql.GTIDSet, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("binlog: empty GTID set")
	}
	if flavor != mysql.MySQLFlavor && flavor != mysql.MariaDBFlavor {
		return nil, fmt.Errorf("binlog: unsupported flavor %q (want mysql or mariadb)", flavor)
	}
	s, err := mysql.ParseGTIDSet(flavor, raw)
	if err != nil {
		return nil, fmt.Errorf("binlog: parse GTID set %q: %w", raw, err)
	}
	return s, nil
}

// MatchGTID 判断单个 GTID（格式 "uuid:seq"）是否落在 set 内。
// 单 GTID 解析失败返回 false（保守：宁可漏匹配不让无效输入通过）。
func MatchGTID(set mysql.GTIDSet, gtid string) bool {
	if set == nil || gtid == "" {
		return false
	}
	// 把单 GTID 当成 1-长度区间
	parts := strings.SplitN(gtid, ":", 2)
	if len(parts) != 2 {
		return false
	}
	singleRange := parts[0] + ":" + parts[1] + "-" + parts[1]
	var sub mysql.GTIDSet
	var err error
	switch set.(type) {
	case *mysql.MariadbGTIDSet:
		sub, err = mysql.ParseMariadbGTIDSet(singleRange)
	default:
		sub, err = mysql.ParseMysqlGTIDSet(singleRange)
	}
	if err != nil {
		return false
	}
	return set.Contain(sub)
}
