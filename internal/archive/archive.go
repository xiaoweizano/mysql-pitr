// Package archive 实现 binlog 归档写入：把 binlog.Source 的事件流还原成
// binlog 文件（先写 .partial，Seal 时解析验证通过才追加/改名为正式文件）。
// 缺口检测（缺失文件回填）由上层归档循环（internal/collector）负责。
//
// 依赖约束：只使用标准库 + go-mysql + internal/binlog 的 Source 接口，
// 不依赖 scan / stream / server 等上层包。
package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/a-shan/mysql-pitr/internal/binlog"
)

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e} // "\xfe\x62\x69\x6e"

// 首个归档文件的默认文件名（无 ROTATE_EVENT 时）。
const defaultBinlogName = "mysql-bin.000001"

// ManifestFile 是 manifest（SHOW BINARY LOGS）中的一个 binlog 文件条目。
type ManifestFile struct {
	Name string
	Size int64
}

// Writer 把事件流写入 dir 下的归档文件。
type Writer struct {
	dir string
}

// NewWriter 创建一个写 dir 目录的 Writer。
func NewWriter(dir string) *Writer { return &Writer{dir: dir} }

// Consume 把事件流写入 dir 下以文件名命名的 .partial 文件（全量重建模式）。
//
// 首个事件前写 magic，默认文件名 mysql-bin.000001；收到 ROTATE_EVENT 时
// 关闭当前文件，并取 RotateEvent.NextLogName 作为下一个文件名（等下一个
// 事件到达才开新文件、写 magic）。
//
// 返回值是段边界信息：遇到**真实轮转**（流起始的公告轮转除外，见下）时
// 返回该轮转的目标文件名（rotate 的 NextLogName）与 nil 错误——调用方应
// Seal 本段写出的 .partial（段起始文件），并把状态推进到返回的文件名；
// 流正常结束（io.EOF）但未发生轮转时返回 ("", nil)。
//
// 流起始的公告轮转（StartSync 时 master 重发的 fake ROTATE_EVENT，目标名
// 即本段要写的文件）不会结束本段：它在任何内容事件之前出现，仅用于给
// 全量重建模式命名目标文件。
func (w *Writer) Consume(ctx context.Context, src binlog.Source) (string, error) {
	return w.consume(ctx, src, "", true)
}

// ConsumeAppend 把事件流续写到指定文件名的 .partial（append 续写模式）。
//
// 与 Consume 的区别：不写 magic（.partial 只含续写尾部的事件字节，Seal 时
// 追加到已封口文件末尾）；文件名由 fileName 参数给定而非 rotate 事件决定。
//
// 返回值与 Consume 相同：真实轮转 → (目标文件名, nil)；EOF 无轮转 → ("", nil)。
func (w *Writer) ConsumeAppend(ctx context.Context, src binlog.Source, fileName string) (string, error) {
	return w.consume(ctx, src, fileName, false)
}

// consume 是 Consume / ConsumeAppend 的共用实现。
// writeMagic=true（全量重建）：首个事件前写 magic，ROTATE_EVENT 更新下一个
// 文件名，事件流从头开始；writeMagic=false（append 续写）：fileName 固定目标
// 文件名（首事件前不写 magic）。
//
// ROTATE_EVENT 语义（两种）：
//   - started==false（本段尚未写任何内容）：流起始公告（fake rotate）——
//     全量重建模式用它命名目标文件（next=name），append 模式忽略，均继续。
//   - started==true：真实轮转——关闭当前文件，段结束，返回目标文件名。
//
// io.EOF 返回 ("", nil)（段未完成，调用方决定丢弃或封口）。
func (w *Writer) consume(ctx context.Context, src binlog.Source, fileName string, writeMagic bool) (string, error) {
	var f *os.File
	next := fileName // 下一个文件名；全量重建模式初始为空 = 默认名
	started := false // 本段是否已写出任何内容
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	for {
		ev, err := src.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", nil // 流正常结束（未轮转，段不完整）
			}
			return "", err
		}
		if ev.Header == nil {
			continue
		}
		if ev.Header.EventType == replication.ROTATE_EVENT {
			name, err := RotateNextLogName(ev)
			if err != nil {
				return "", err
			}
			if err := validateRotateName(name); err != nil {
				return "", err
			}
			if !started {
				// 流起始公告（fake rotate）：仅命名/忽略，段继续
				if writeMagic {
					next = name
				}
				continue
			}
			// 真实轮转：关闭当前文件，段结束，返回下一个文件名
			if f != nil {
				f.Close()
				f = nil
			}
			return name, nil
		}
		if f == nil {
			name := next
			if name == "" {
				name = defaultBinlogName
			}
			path := filepath.Join(w.dir, name+".partial")
			if writeMagic {
				// 全量重建：截断重建，写入 magic
				if f, err = os.Create(path); err != nil {
					return "", err
				}
				if _, err := f.Write(binlogMagic); err != nil {
					return "", err
				}
			} else {
				// append 续写：只追加（不写 magic）。O_APPEND 让 Seal 失败后
				// 残留的旧 partial 可以安全续写（重试语义），不重复写入。
				if f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644); err != nil {
					return "", err
				}
			}
			started = true
		}
		if _, err := f.Write(ev.RawData); err != nil {
			return "", err
		}
	}
}

// validateRotateName 校验 ROTATE_EVENT 给出的下一个文件名可以安全地作为归档
// 目录下的文件名。恶意/损坏事件携带的 "../evil"、"C:\evil"、空串等必须拒绝，
// 否则 filepath.Join(w.dir, name+".partial") 会把文件写出归档目录（final
// review Important #4）。MySQL 的 binlog 文件名恒为 "<前缀>.<全数字>"
// （如 mysql-bin.000002），因此该模式可作为强校验。
func validateRotateName(name string) error {
	if name == "" {
		return fmt.Errorf("archive: rotate next log name is empty")
	}
	if name != filepath.Base(name) || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("archive: rotate next log name %q contains path separators (path traversal rejected)", name)
	}
	i := strings.LastIndex(name, ".")
	if i <= 0 || i == len(name)-1 {
		return fmt.Errorf("archive: rotate next log name %q does not match binlog naming (<prefix>.<digits>)", name)
	}
	for _, c := range name[i+1:] {
		if c < '0' || c > '9' {
			return fmt.Errorf("archive: rotate next log name %q does not match binlog naming (<prefix>.<digits>)", name)
		}
	}
	return nil
}

// RotateNextLogName 从 ROTATE_EVENT 提取下一个文件名。
// 优先用已解析的 RotateEvent（真实流）；测试 stub 等来源不填 ev.Event，
// 则从 RawData 解析（19B header + 8B position + name + 4B CRC32）。
func RotateNextLogName(ev *replication.BinlogEvent) (string, error) {
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

// Seal 封口 .partial 文件，语义取决于内容（以 binlog magic 开头与否）：
//
//   - 全量重建（magic 开头）：目标文件已存在 → 拒绝（防覆盖已封口文件）；
//     否则用 verifyParseable 验证（含 FDE 时启用校验和验证），通过后 rename。
//   - append 续写（无 magic）：目标文件必须已存在；magic+tail 拼临时文件验证
//     可解析后追加到目标文件末尾，删除 .partial。
//
// 验证失败返回错误，调用方回退整文件拷贝。
//
// 注：go-mysql 的校验和验证以 FDE 解析为门槛——无 FDE 的内容（如 append 尾部、
// 旋转产生的后续文件，只含 magic + XID）会跳过 CRC 校验但仍能解析封口，这是
// go-mysql 语义，非本包可控制；尾部的事件结构破坏（长度/截断）仍会被捕获。
func (w *Writer) Seal(partialName string) error {
	src := filepath.Join(w.dir, partialName)
	final := strings.TrimSuffix(src, ".partial")

	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("archive: read partial %s: %w", partialName, err)
	}
	_, statErr := os.Stat(final)
	finalExists := statErr == nil

	if len(data) >= 4 && string(data[:4]) == string(binlogMagic) {
		// 全量重建：目标已存在 → 拒绝（防覆盖）
		if finalExists {
			return fmt.Errorf("archive: refuse full reconstruction over sealed file %s", filepath.Base(final))
		}
		if err := w.verifyParseable(data[4:]); err != nil {
			return fmt.Errorf("archive: seal verify %s: %w", partialName, err)
		}
		return os.Rename(src, final)
	}

	// append 续写：目标必须已存在；magic+tail 验证后追加
	if !finalExists {
		return fmt.Errorf("archive: append seal %s but final %s missing", partialName, filepath.Base(final))
	}
	if err := w.verifyParseable(data); err != nil {
		return fmt.Errorf("archive: append seal verify %s: %w", partialName, err)
	}
	f, err := os.OpenFile(final, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("archive: open final %s: %w", partialName, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("archive: append %s: %w", partialName, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("archive: close final: %w", err)
	}
	return os.Remove(src)
}

// SealAppendVerified 是 append 封口的 CRC 强化版本（T5/T4 评审 carry-in）：
// 把「最终文件全部内容 + 尾部」拼临时文件做整体 ParseFile + SetVerifyChecksum，
// 验证通过才把尾部追加到最终文件末尾、删除 .partial。
//
// 与 Seal 的 append 分支（magic+tail 单独验证）区别：组合验证里 FDE 存在，
// 使 go-mysql 对每个事件的 CRC32 校验生效——即使篡改最终文件内部的一个事件
// 字节也会被捕获（Seal 的 append 分支因尾部无 FDE 会跳过 CRC）。
//
// 前置条件：最终文件必须已存在（本方法不做回填）；partial 可含任意事件字节。
func (w *Writer) SealAppendVerified(partialName string) error {
	src := filepath.Join(w.dir, partialName)
	final := strings.TrimSuffix(src, ".partial")

	tail, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("archive: read partial %s: %w", partialName, err)
	}
	if _, err := os.Stat(final); err != nil {
		return fmt.Errorf("archive: append seal %s but final %s missing", partialName, filepath.Base(final))
	}

	// 追加幂等（I1）：若最终文件末尾已含有与 partial 逐字节相同的尾部，
	// 说明这段尾部此前已成功追加（Seal 成功但 SaveState 失败 / 崩溃窗口后
	// 从旧位置重拉）——跳过追加、直接清理 partial。final 是连续前缀时
	// 「末尾 == tail」⟹ tail 的事件已在 final 中，跳过不丢失任何事件。
	if alreadyAppended(final, tail) {
		return os.Remove(src)
	}

	// 组合验证：临时文件 = final 内容（含 magic）+ tail（事件字节），整体解析。
	// 用流式拷贝而非一次性 ReadFile，避免 1GB 级文件双份内存。
	tmp := filepath.Join(w.dir, ".verify-"+fmt.Sprint(time.Now().UnixNano()))
	tf, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	fr, err := os.Open(final)
	if err != nil {
		tf.Close()
		return fmt.Errorf("archive: open final %s: %w", partialName, err)
	}
	if _, err := io.Copy(tf, fr); err != nil {
		fr.Close()
		tf.Close()
		return fmt.Errorf("archive: copy final %s: %w", partialName, err)
	}
	fr.Close()
	if _, err := tf.Write(tail); err != nil {
		tf.Close()
		return fmt.Errorf("archive: write tail %s: %w", partialName, err)
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("archive: close verify tmp: %w", err)
	}

	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	if err := parser.ParseFile(tmp, 0, func(*replication.BinlogEvent) error { return nil }); err != nil {
		return fmt.Errorf("archive: append seal verify %s: %w", partialName, err)
	}

	// 验证通过：追加尾部到最终文件
	f, err := os.OpenFile(final, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("archive: open final %s: %w", partialName, err)
	}
	if _, err := f.Write(tail); err != nil {
		f.Close()
		return fmt.Errorf("archive: append %s: %w", partialName, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("archive: close final: %w", err)
	}
	return os.Remove(src)
}

// alreadyAppended 判断 final 的末尾 len(tail) 字节是否与 tail 逐字节相同
// （即这段尾部此前已被追加过）。
func alreadyAppended(final string, tail []byte) bool {
	if len(tail) == 0 {
		return false // 空尾部无意义；正常路径不产生空 partial
	}
	f, err := os.Open(final)
	if err != nil {
		return false
	}
	defer f.Close()
	if _, err := f.Seek(-int64(len(tail)), io.SeekEnd); err != nil {
		return false
	}
	buf := make([]byte, len(tail))
	if _, err := io.ReadFull(f, buf); err != nil {
		return false
	}
	return bytes.Equal(buf, tail)
}

// verifyParseable 用 magic+data 拼临时文件做 ParseFile + SetVerifyChecksum(true)，
// 验证 data 可以作为 binlog 文件的开头之后（紧跟 magic）完整解析。
// 调用方传入的内容必须不含 magic（全量重建分支先剥离已写入的 magic）。
func (w *Writer) verifyParseable(data []byte) error {
	tmp := filepath.Join(w.dir, ".verify-"+fmt.Sprint(time.Now().UnixNano()))
	if err := os.WriteFile(tmp, append(append([]byte{}, binlogMagic...), data...), 0o644); err != nil {
		return err
	}
	defer os.Remove(tmp)
	parser := replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	return parser.ParseFile(tmp, 0, func(*replication.BinlogEvent) error { return nil })
}
