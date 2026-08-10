package binlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// EnumerateBinlogFiles 列出目录内的 binlog 文件，按文件名排序。
// 当 startPos.Name 非空时，从该文件开始（含）。
// 当 endPos.Name 非空时，到该文件结束（含）。
// 空 startPos 表示从最早文件开始；空 endPos 表示到最新文件。
func EnumerateBinlogFiles(dir string, startPos, endPos mysql.Position) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("binlog: read dir %q: %w", dir, err)
	}

	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !isBinlogFile(e.Name()) {
			continue
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("binlog: no binlog files in %q", dir)
	}
	sort.Strings(names)

	startName := startPos.Name
	endName := endPos.Name

	// 先在整个有序列表上求 start/end 的下标，再统一切片，
	// 这样当 start 排在 end 之后时能正确报错（若先按 start 切片，
	// end 会从剩余列表中消失，无法给出准确的顺序错误）。
	lo := 0
	if startName != "" {
		si := indexOf(names, startName)
		if si < 0 {
			return nil, fmt.Errorf("binlog: start file %q not found", startName)
		}
		lo = si
	}
	hi := len(names) - 1
	if endName != "" {
		ei := indexOf(names, endName)
		if ei < 0 {
			return nil, fmt.Errorf("binlog: end file %q not found", endName)
		}
		hi = ei
	}
	if lo > hi {
		return nil, fmt.Errorf("binlog: start file after end (start %q, end %q)", startName, endName)
	}
	names = names[lo : hi+1]

	out := make([]string, len(names))
	for i, n := range names {
		out[i] = filepath.Join(dir, n)
	}
	return out, nil
}

// isBinlogFile 判断文件名是否匹配 binlog 命名（如 mysql-bin.000001）。
// 实现宽松：包含 ".<数字>" 后缀即可。
func isBinlogFile(name string) bool {
	// 形如 prefix.NNNNNN
	i := strings.LastIndex(name, ".")
	if i < 0 || i == len(name)-1 {
		return false
	}
	suffix := name[i+1:]
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(suffix) > 0
}

func indexOf(names []string, target string) int {
	for i, n := range names {
		if n == target {
			return i
		}
	}
	return -1
}
