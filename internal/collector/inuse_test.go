package collector

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/a-shan/mysql-pitr/internal/binlogtest"
)

var testMagic = []byte{0xfe, 0x62, 0x69, 0x6e}

// parseStrict 以与扫描引擎一致的严格校验解析文件。
func parseStrict(t *testing.T, path string) error {
	t.Helper()
	p := replication.NewBinlogParser()
	p.SetVerifyChecksum(true)
	return p.ParseFile(path, 0, func(*replication.BinlogEvent) error { return nil })
}

// mimicActiveFDE 把一致的 FDE 改成 mysqld 活跃文件形态:置位
// LOG_EVENT_BINLOG_IN_USE_F 但不重算 CRC(等价 mysqld 的直接字节回写)。
func mimicActiveFDE(fde []byte) []byte {
	out := append([]byte(nil), fde...)
	binary.LittleEndian.PutUint16(out[17:], replication.LOG_EVENT_BINLOG_IN_USE_F)
	return out
}

func writeFile(t *testing.T, path string, fde []byte) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, append(append([]byte{}, testMagic...), fde...), 0o644))
}

// 线上问题的回归测试:活跃文件的回填副本必须能通过严格校验。
func TestCopyFilePrefix_ClearsInUseFlag(t *testing.T) {
	fde := binlogtest.MustCraft(binlogtest.CraftFDE()).Raw

	src := filepath.Join(t.TempDir(), "mysql-bin.000001")
	dst := filepath.Join(t.TempDir(), "archive", "mysql-bin.000001")
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
	writeFile(t, src, mimicActiveFDE(fde))

	require.Error(t, parseStrict(t, src), "active-style FDE must fail strict checksum (production symptom)")

	require.NoError(t, copyFilePrefix(src, dst, -1))

	require.NoError(t, parseStrict(t, dst), "backfilled copy must pass strict checksum")
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, uint16(0), binary.LittleEndian.Uint16(data[4+17:4+19]),
		"in-use flag must be cleared in the archived copy")
}

// 一致文件的副本必须逐字节不变。
func TestCopyFilePrefix_KeepsConsistentFDE(t *testing.T) {
	fde := binlogtest.MustCraft(binlogtest.CraftFDE()).Raw

	src := filepath.Join(t.TempDir(), "mysql-bin.000001")
	dst := filepath.Join(t.TempDir(), "archive", "mysql-bin.000001")
	require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
	writeFile(t, src, fde)

	require.NoError(t, copyFilePrefix(src, dst, -1))

	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.True(t, bytes.Equal(append(append([]byte{}, testMagic...), fde...), data),
		"consistent file must be copied byte-identically")
	require.NoError(t, parseStrict(t, dst))
}

// 真损坏(字节被改)不得被 in-use 清零掩盖。
func TestClearInUseFlag_GenuineCorruptionUntouched(t *testing.T) {
	corrupt := binlogtest.MustCraft(binlogtest.CraftFDE()).Raw
	corrupt[40] ^= 0xff                                  // 中部字节损坏
	binary.LittleEndian.PutUint16(corrupt[17:], 0x0001)  // 同时带 in-use 位
	path := filepath.Join(t.TempDir(), "mysql-bin.000001")
	writeFile(t, path, corrupt)

	require.NoError(t, clearInUseFlag(path))
	require.Error(t, parseStrict(t, path), "genuine corruption must not be masked")
}
