// Package archive 实现 binlog 归档写入：把 binlog.Source 的事件流还原成
// binlog 文件（先写 .partial，Seal 时解析验证通过才改名为正式文件），
// 并提供与 manifest（SHOW BINARY LOGS 的抽象）对照的缺口检测。
//
// 依赖约束：只使用标准库 + go-mysql + internal/binlog 的 Source 接口，
// 不依赖 scan / stream / server 等上层包。
package archive

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e} // "\xfe\x62\x69\x6e"

// 首个归档文件的默认文件名（无 ROTATE_EVENT 时）。
const defaultBinlogName = "mysql-bin.000001"

// ManifestFile 是 manifest 中的一个 binlog 文件条目。
type ManifestFile struct {
	Name string
	Size int64
}

// Manifest 列出 MySQL 当前持有的 binlog 文件（SHOW BINARY LOGS 的抽象）。
type Manifest interface {
	List(ctx context.Context) ([]ManifestFile, error)
}

// Writer 把事件流写入 dir 下的归档文件。
type Writer struct {
	dir string
}

// NewWriter 创建一个写 dir 目录的 Writer。
func NewWriter(dir string) *Writer { return &Writer{dir: dir} }

// Consume 把事件流写入 dir 下以文件名命名的 .partial 文件。
//
// 首个事件前写 magic，默认文件名 mysql-bin.000001；收到 ROTATE_EVENT 时
// 关闭当前文件，并取 RotateEvent.NextLogName 作为下一个文件名（等下一个
// 事件到达才开新文件、写 magic）。
func (w *Writer) Consume(ctx context.Context, src binlog.Source) error {
	var f *os.File
	var next string // 最近一次 ROTATE_EVENT 给出的下一个文件名；空 = 默认名
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	for {
		ev, err := src.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil // 流正常结束
			}
			return err
		}
		if ev.Header == nil {
			continue
		}
		if ev.Header.EventType == replication.ROTATE_EVENT {
			name, err := rotateNextLogName(ev)
			if err != nil {
				return err
			}
			next = name
			if f != nil {
				f.Close()
				f = nil
			}
			continue
		}
		if f == nil {
			name := next
			if name == "" {
				name = defaultBinlogName
			}
			path := filepath.Join(w.dir, name+".partial")
			if f, err = os.Create(path); err != nil {
				return err
			}
			if _, err := f.Write(binlogMagic); err != nil {
				return err
			}
		}
		if _, err := f.Write(ev.RawData); err != nil {
			return err
		}
	}
}

// rotateNextLogName 从 ROTATE_EVENT 提取下一个文件名。
// 优先用已解析的 RotateEvent（真实流）；测试 stub 等来源不填 ev.Event，
// 则从 RawData 解析（19B header + 8B position + name + 4B CRC32）。
func rotateNextLogName(ev *replication.BinlogEvent) (string, error) {
	if re, ok := ev.Event.(*replication.RotateEvent); ok && len(re.NextLogName) > 0 {
		return string(re.NextLogName), nil
	}
	raw := ev.RawData
	const nameOffset = replication.EventHeaderSize + 8
	if len(raw) >= nameOffset+replication.BinlogChecksumLength {
		name := string(raw[nameOffset : len(raw)-replication.BinlogChecksumLength])
		if name != "" {
			return name, nil
		}
	}
	return "", fmt.Errorf("archive: rotate event without next log name")
}

// Seal 用 ParseFile + SetVerifyChecksum(true) 验证 .partial 可完整解析，
// 通过则去掉 .partial 后缀改名为正式文件；验证失败返回错误，调用方回退
// 整文件拷贝。
//
// 注：go-mysql 的校验和验证以 FDE 解析为门槛——无 FDE 的文件（如旋转产生的
// 后续文件，只含 magic + XID）会跳过 CRC 校验但仍能解析封口，这是 go-mysql
// 语义，非本包可控制。
func (w *Writer) Seal(partialName string) error {
	src := filepath.Join(w.dir, partialName)
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	if err := parser.ParseFile(src, 0, func(*replication.BinlogEvent) error { return nil }); err != nil {
		return fmt.Errorf("archive: seal verify %s: %w", partialName, err)
	}
	final := strings.TrimSuffix(src, ".partial")
	if err := os.Rename(src, final); err != nil {
		return fmt.Errorf("archive: rename %s: %w", partialName, err)
	}
	return nil
}

// Gaps 对比 manifest，找出本地归档中完全缺失的文件（Size 不匹配的
// 「部分归档」场景由 Phase 2 的 reconcile 处理）。
func (w *Writer) Gaps(ctx context.Context, m Manifest) ([]string, error) {
	files, err := m.List(ctx)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, mf := range files {
		final := filepath.Join(w.dir, mf.Name)
		if _, err := os.Stat(final); os.IsNotExist(err) {
			missing = append(missing, mf.Name)
		}
	}
	sort.Strings(missing)
	return missing, nil
}
