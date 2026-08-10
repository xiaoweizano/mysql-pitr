package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGTIDSet_MySQL(t *testing.T) {
	s, err := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5")
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Contains(t, s.String(), "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5")
}

func TestParseGTIDSet_Empty(t *testing.T) {
	_, err := ParseGTIDSet("mysql", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestParseGTIDSet_InvalidFlavor(t *testing.T) {
	_, err := ParseGTIDSet("oracle", "x:1")
	require.Error(t, err)
}

func TestMatchGTID_Inside(t *testing.T) {
	s, _ := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-10")
	assert.True(t, MatchGTID(s, "de278ad0-2106-11e4-9f8e-6edd0ca20947:5"))
}

func TestMatchGTID_OutsideRange(t *testing.T) {
	s, _ := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-10")
	assert.False(t, MatchGTID(s, "de278ad0-2106-11e4-9f8e-6edd0ca20947:15"))
}

func TestMatchGTID_DifferentUUID(t *testing.T) {
	s, _ := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-10")
	assert.False(t, MatchGTID(s, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:5"))
}

func TestMatchGTID_MultiIntervalSet(t *testing.T) {
	s, _ := ParseGTIDSet("mysql",
		"de278ad0-2106-11e4-9f8e-6edd0ca20947:1-5:20-30")
	assert.True(t, MatchGTID(s, "de278ad0-2106-11e4-9f8e-6edd0ca20947:25"))
	assert.False(t, MatchGTID(s, "de278ad0-2106-11e4-9f8e-6edd0ca20947:15"))
}

func TestParseGTIDSet_MariaDB(t *testing.T) {
	s, err := ParseGTIDSet("mariadb", "1-2-3")
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Contains(t, s.String(), "1-2-3")
}

func TestParseGTIDSet_InvalidRaw(t *testing.T) {
	_, err := ParseGTIDSet("mysql", "garbage!!!not-a-gtid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse GTID set")
}

func TestMatchGTID_NilSet(t *testing.T) {
	assert.False(t, MatchGTID(nil, "de278ad0-2106-11e4-9f8e-6edd0ca20947:5"))
}

func TestMatchGTID_EmptyGTID(t *testing.T) {
	s, _ := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-10")
	assert.False(t, MatchGTID(s, ""))
}

func TestMatchGTID_MalformedGTID(t *testing.T) {
	s, _ := ParseGTIDSet("mysql", "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-10")
	// 无冒号 → 不是 uuid:gno 格式
	assert.False(t, MatchGTID(s, "de278ad0-2106-11e4-9f8e-6edd0ca20947"))
	// 单 GTID 无法解析 → 保守返回 false
	assert.False(t, MatchGTID(s, "garbage:garbage"))
}

func TestMatchGTID_MariaDBSet(t *testing.T) {
	s, _ := ParseGTIDSet("mariadb", "1-2-3")
	// MariaDB GTID 无冒号 → 走 split 失败路径
	assert.False(t, MatchGTID(s, "1-2-3"))
	// 带冒号会进入 MariaDB 单范围解析 → 无法解析 → false
	assert.False(t, MatchGTID(s, "1:3"))
}
