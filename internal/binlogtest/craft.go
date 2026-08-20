// Package binlogtest 提供手工构造 binlog 字节流的测试工具。
//
// 这些 helpers 原位于 internal/binlog/coverage_test.go 的 craft* 系列，
// 迁出并导出化，供 binlog / archive / stream 等包的测试共用。
package binlogtest

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// 默认时间戳与 server id：与 binlog 包 coverage 测试的断言
// （CommitTime == time.Unix(1750000000, 0)）保持一致。
const (
	defaultTimestamp = uint32(1750000000)
	defaultServerID  = uint32(1)
	defaultVersion   = "8.0.36"
)

// Event 是一个手工构造的 binlog 事件：类型 + 原始字节（header + body + CRC32）。
type Event struct {
	Type replication.EventType
	Raw  []byte
}

// MustCraft 把 (ev, err) 包装为 Event；err != nil 或事件过短时 panic（仅测试用）。
// 事件类型从原始字节的第 4 字节（header 中 EventType 位置）提取。
func MustCraft(ev []byte, err error) Event {
	if err != nil {
		panic(err)
	}
	if len(ev) < replication.EventHeaderSize {
		panic("binlogtest: crafted event too short")
	}
	return Event{Type: replication.EventType(ev[4]), Raw: ev}
}

// WithTimestamp 改写已构造事件 header 中的时间戳并重算 CRC32，
// 供 TimeRange 过滤/提前终止类测试构造差异化提交时间的事务。
func WithTimestamp(ts uint32, ev []byte) []byte {
	out := append([]byte(nil), ev...)
	binary.LittleEndian.PutUint32(out[0:], ts)
	body := out[:len(out)-replication.BinlogChecksumLength]
	binary.LittleEndian.PutUint32(out[len(out)-replication.BinlogChecksumLength:], crc32.ChecksumIEEE(body))
	return out
}

// craftEvent 构造一个完整 binlog 事件：19 字节 header + body + CRC32。
// 校验和按 MySQL 规范覆盖 header+body（与 parser.SetVerifyChecksum(true) 一致）。
func craftEvent(ts uint32, etype replication.EventType, serverID uint32, body []byte) []byte {
	ev := make([]byte, replication.EventHeaderSize,
		replication.EventHeaderSize+len(body)+replication.BinlogChecksumLength)
	binary.LittleEndian.PutUint32(ev[0:], ts)
	ev[4] = byte(etype)
	binary.LittleEndian.PutUint32(ev[5:], serverID)
	binary.LittleEndian.PutUint32(ev[9:], uint32(replication.EventHeaderSize+len(body)+replication.BinlogChecksumLength))
	// LogPos 不参与 go-mysql 的校验，保持 0
	binary.LittleEndian.PutUint16(ev[17:], 0)
	ev = append(ev, body...)
	crc := crc32.ChecksumIEEE(ev)
	var cb [replication.BinlogChecksumLength]byte
	binary.LittleEndian.PutUint32(cb[:], crc)
	return append(ev, cb[:]...)
}

// CraftFDE 构造 FormatDescriptionEvent（版本 8.0.36，CRC32 校验和）。
// 事件类型 header 长度数组全部填 8 → go-mysql 用 6 字节 table id。
func CraftFDE() ([]byte, error) {
	body := make([]byte, 0, 2+50+4+1+42+1)
	body = binary.LittleEndian.AppendUint16(body, 4) // binlog version
	sv := make([]byte, 50)
	copy(sv, defaultVersion)
	body = append(body, sv...)
	body = binary.LittleEndian.AppendUint32(body, 0) // create timestamp
	body = append(body, replication.EventHeaderSize) // header length = 19
	body = append(body, bytes.Repeat([]byte{8}, 42)...)
	body = append(body, byte(replication.BINLOG_CHECKSUM_ALG_CRC32))
	return craftEvent(defaultTimestamp, replication.FORMAT_DESCRIPTION_EVENT, defaultServerID, body), nil
}

// CraftQuery 构造 QUERY_EVENT。schema 可选（缺省为空字符串）；
// binlog 包 scanner 在 BEGIN 时用 QueryEvent.Schema 记录事务 schema，
// 需要时显式传入（如 CraftQuery("BEGIN", "shop")）。
func CraftQuery(q string, schema ...string) ([]byte, error) {
	s := ""
	if len(schema) > 0 {
		s = schema[0]
	}
	body := make([]byte, 0, 13+len(s)+1+len(q))
	body = binary.LittleEndian.AppendUint32(body, 1) // thread id
	body = binary.LittleEndian.AppendUint32(body, 0) // exec time
	body = append(body, byte(len(s)))
	body = binary.LittleEndian.AppendUint16(body, 0) // error code
	body = binary.LittleEndian.AppendUint16(body, 0) // status vars length
	body = append(body, s...)
	body = append(body, 0) // 结尾 NUL
	body = append(body, q...)
	return craftEvent(defaultTimestamp, replication.QUERY_EVENT, defaultServerID, body), nil
}

// CraftXID 构造 XID_EVENT。
func CraftXID(xid uint64) ([]byte, error) {
	body := make([]byte, 8)
	binary.LittleEndian.PutUint64(body, xid)
	return craftEvent(defaultTimestamp, replication.XID_EVENT, defaultServerID, body), nil
}

// CraftGTID 构造 GTID_EVENT。sid 接受 32/36 位 hex（含 '-' 的 UUID 形式）并解码为
// 16 字节；其他字符串按原始字节拷贝并右补 0 到 16 字节。空串表示 Anonymous GTID。
func CraftGTID(sid string, gno int64) ([]byte, error) {
	body := make([]byte, 0, 1+16+8)
	body = append(body, 1) // commit flag
	body = append(body, parseSID(sid)...)
	body = binary.LittleEndian.AppendUint64(body, uint64(gno))
	return craftEvent(defaultTimestamp, replication.GTID_EVENT, defaultServerID, body), nil
}

// parseSID 把 sid 字符串转成 16 字节 SID。
func parseSID(s string) []byte {
	out := make([]byte, 16)
	clean := strings.ReplaceAll(s, "-", "")
	if len(clean) == 32 {
		if b, err := hex.DecodeString(clean); err == nil {
			copy(out, b)
			return out
		}
	}
	copy(out, s)
	return out
}

// CraftMariaDBGTID 构造 MARIADB_GTID_EVENT（13 字节 body：seq + domain + flags）。
func CraftMariaDBGTID(seq uint64, domain uint32, flags byte) ([]byte, error) {
	body := make([]byte, 13)
	binary.LittleEndian.PutUint64(body[0:], seq)
	binary.LittleEndian.PutUint32(body[8:], domain)
	body[12] = flags
	return craftEvent(defaultTimestamp, replication.MARIADB_GTID_EVENT, defaultServerID, body), nil
}

// CraftTableMap 构造 1 列（LONGLONG）的 TABLE_MAP_EVENT。
func CraftTableMap(schema, table string, tableID uint64) ([]byte, error) {
	body := make([]byte, 0, 32)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = append(body, byte(len(schema)))
	body = append(body, schema...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, table...)
	body = append(body, 0)
	body = append(body, 1) // column count (lenenc)
	body = append(body, mysql.MYSQL_TYPE_LONGLONG)
	body = append(body, 1, 0) // meta: lenenc 长度 1，值 0
	body = append(body, 0)    // null bitmap
	return craftEvent(defaultTimestamp, replication.TABLE_MAP_EVENT, defaultServerID, body), nil
}

// CraftWriteRows 构造 WRITE_ROWS_EVENTv2（单列 LONGLONG），n 行，行值 1..n。
func CraftWriteRows(tableID uint64, n int) ([]byte, error) {
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(i + 1)
	}
	return CraftWriteRowsValues(tableID, vals...)
}

// CraftWriteRowsValues 构造 WRITE_ROWS_EVENTv2（单列 LONGLONG），行值由调用方指定。
func CraftWriteRowsValues(tableID uint64, values ...int64) ([]byte, error) {
	body := make([]byte, 0, 6+2+2+2+len(values)*9)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = binary.LittleEndian.AppendUint16(body, 2) // extra data length（无）
	body = append(body, 1)                           // column count
	body = append(body, 0x01)                        // bitmap1：第 0 列存在
	for _, v := range values {
		body = append(body, 0x00) // null bitmap：第 0 列非 NULL
		body = binary.LittleEndian.AppendUint64(body, uint64(v))
	}
	return craftEvent(defaultTimestamp, replication.WRITE_ROWS_EVENTv2, defaultServerID, body), nil
}

// CraftUpdateRows 构造 UPDATE_ROWS_EVENTv2（单列 LONGLONG），n 对 before/after 行，
// before 值 1..n、after 值 n+1..2n。
//
// 布局按行交错：before(1), after(1), before(2), after(2), ...，与 go-mysql
// RowsEvent.DecodeData 的逐行两镜像解析（ColumnBitmap1 解 before、ColumnBitmap2
// 解 after）一致；若先输出全部 before 再输出全部 after，n≥2 时行会被静默错配。
func CraftUpdateRows(tableID uint64, n int) ([]byte, error) {
	body := make([]byte, 0, 6+2+2+1+1+1+n*18)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = binary.LittleEndian.AppendUint16(body, 2) // extra data length（无）
	body = append(body, 1)                           // column count
	body = append(body, 0x01)                        // bitmap1：before 第 0 列存在
	body = append(body, 0x01)                        // bitmap2：after 第 0 列存在
	for i := 0; i < n; i++ {
		body = append(body, 0x00) // before null bitmap
		body = binary.LittleEndian.AppendUint64(body, uint64(i+1))
		body = append(body, 0x00) // after null bitmap
		body = binary.LittleEndian.AppendUint64(body, uint64(n+i+1))
	}
	return craftEvent(defaultTimestamp, replication.UPDATE_ROWS_EVENTv2, defaultServerID, body), nil
}

// CraftDeleteRows 构造 DELETE_ROWS_EVENTv2（单列 LONGLONG），n 行，行值 1..n。
func CraftDeleteRows(tableID uint64, n int) ([]byte, error) {
	body := make([]byte, 0, 6+2+2+2+n*9)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = binary.LittleEndian.AppendUint16(body, 2) // extra data length（无）
	body = append(body, 1)                           // column count
	body = append(body, 0x01)                        // bitmap1：第 0 列存在
	for i := 0; i < n; i++ {
		body = append(body, 0x00) // null bitmap：第 0 列非 NULL
		body = binary.LittleEndian.AppendUint64(body, uint64(i+1))
	}
	return craftEvent(defaultTimestamp, replication.DELETE_ROWS_EVENTv2, defaultServerID, body), nil
}

// CraftPartialUpdateRows 构造 PARTIAL_UPDATE_ROWS_EVENT 的 body。
// before/after image 各 1 行；after image 前缀是 binlog_row_value_options（0 = 非 partial JSON）。
func CraftPartialUpdateRows(tableID uint64, before, after int64) ([]byte, error) {
	body := make([]byte, 0, 6+2+2+2+19)
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, tableID)
	body = append(body, id[:6]...)
	body = binary.LittleEndian.AppendUint16(body, 0) // flags
	body = binary.LittleEndian.AppendUint16(body, 2) // extra data length（无）
	body = append(body, 1)                           // column count
	body = append(body, 0x01)                        // bitmap1
	body = append(body, 0x01)                        // bitmap2
	body = append(body, 0x00)                        // before: null bitmap
	body = binary.LittleEndian.AppendUint64(body, uint64(before))
	body = append(body, 0x00) // after: binlog_row_value_options = 0
	body = append(body, 0x00) // after: null bitmap
	body = binary.LittleEndian.AppendUint64(body, uint64(after))
	return craftEvent(defaultTimestamp, replication.PARTIAL_UPDATE_ROWS_EVENT, defaultServerID, body), nil
}

// CraftRotate 构造 ROTATE_EVENT（8 字节 position + next log name）。
func CraftRotate(nextName string) ([]byte, error) {
	body := make([]byte, 0, 8+len(nextName))
	body = binary.LittleEndian.AppendUint64(body, 4) // position（下一个文件的开头）
	body = append(body, nextName...)
	return craftEvent(defaultTimestamp, replication.ROTATE_EVENT, defaultServerID, body), nil
}

// CraftFile 把事件序列拼成完整 binlog 文件字节（magic + 各事件原样拼接）。
// 注意：不自动追加 FDE——调用方需把 CraftFDE() 作为第一个事件显式传入。
func CraftFile(events []Event) []byte {
	out := make([]byte, 0)
	out = append(out, replication.BinLogFileHeader...)
	for _, e := range events {
		out = append(out, e.Raw...)
	}
	return out
}
