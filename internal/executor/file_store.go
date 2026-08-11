package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// FileCheckpointStore 把检查点存为 <dir>/<operationID>.json，原子写入（临时文件+rename）。
type FileCheckpointStore struct{ dir string }

// NewFileCheckpointStore 创建基于目录的文件检查点存储。
func NewFileCheckpointStore(dir string) *FileCheckpointStore { return &FileCheckpointStore{dir: dir} }

func (s *FileCheckpointStore) path(id string) string {
	return filepath.Join(s.dir, id+".json")
}

func (s *FileCheckpointStore) Load(operationID string) (*Checkpoint, error) {
	data, err := os.ReadFile(s.path(operationID))
	if err != nil {
		if os.IsNotExist(err) {
			// 无检查点文件 = 从未跑过（Resume 据此从 0 全跑），不是加载错误。
			return nil, fmt.Errorf("%w (operation %q)", ErrCheckpointNotFound, operationID)
		}
		return nil, fmt.Errorf("executor: load checkpoint %s: %w", operationID, err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("executor: parse checkpoint %s: %w", operationID, err)
	}
	return &cp, nil
}

func (s *FileCheckpointStore) Save(c Checkpoint) error {
	if c.OperationID == "" {
		return fmt.Errorf("executor: checkpoint.OperationID required")
	}
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("executor: marshal checkpoint: %w", err)
	}
	tmp := s.path(c.OperationID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("executor: write checkpoint: %w", err)
	}
	if err := os.Rename(tmp, s.path(c.OperationID)); err != nil {
		return fmt.Errorf("executor: commit checkpoint: %w", err)
	}
	return nil
}

func (s *FileCheckpointStore) Clear(operationID string) error {
	err := os.Remove(s.path(operationID))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("executor: clear checkpoint %s: %w", operationID, err)
	}
	return nil
}
