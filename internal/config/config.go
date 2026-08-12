package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/a-shan/mysql-pitr/internal/connector"
)

// MySQLConfig holds MySQL connection details stored in the encrypted config file.
type MySQLConfig struct {
	Host     string            `json:"host"`
	Port     int               `json:"port"`
	User     string            `json:"user"`
	Password string            `json:"password"`
	Database string            `json:"database"`
	Params   map[string]string `json:"params,omitempty"`
}

// BuildConnConfig converts an MySQLConfig into a connector.ConnConfig.
func (m MySQLConfig) BuildConnConfig() connector.ConnConfig {
	host := m.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := m.Port
	if port == 0 {
		port = 3306
	}
	return connector.ConnConfig{
		Host:     host,
		Port:     port,
		User:     m.User,
		Password: m.Password,
		Database: m.Database,
		Params:   m.Params,
	}
}

// BuildDSN builds a MySQL DSN string from the MySQLConfig fields.
func (m MySQLConfig) BuildDSN() string {
	host := m.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := m.Port
	if port == 0 {
		port = 3306
	}
	cfg := mysql.NewConfig()
	cfg.User = m.User
	cfg.Passwd = m.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.DBName = m.Database

	if m.Params != nil {
		if cfg.Params == nil {
			cfg.Params = make(map[string]string, len(m.Params))
		}
		for k, v := range m.Params {
			cfg.Params[k] = v
		}
	}

	return cfg.FormatDSN()
}

// ServerConfig holds WebSocket server connection details stored in the config file.
type ServerConfig struct {
	URL      string `json:"url"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
	CAFile   string `json:"ca_file"`
}

// ArchiveConfig 归档采集配置（agent serve 的归档循环使用）。
type ArchiveConfig struct {
	Dir           string `json:"dir"`                      // 归档目录（必填）
	ServerID      uint32 `json:"server_id"`                // syncer server id（必填，须 > 0）
	RetentionDays int    `json:"retention_days,omitempty"` // 0 = 不清理
}

// Config represents the full agent configuration file, typically stored encrypted
// on disk and decrypted at startup with a user-supplied passphrase.
type Config struct {
	MySQL   MySQLConfig  `json:"mysql"`
	Server  ServerConfig `json:"server,omitempty"`
	DataDir string       `json:"data_dir"`

	// Archive 归档采集配置（binlog 归档循环的目录、syncer id、保留天数）。
	// 指针使未配置时序列化省略整个 archive 段。
	Archive *ArchiveConfig `json:"archive,omitempty"`

	// BinlogDir overrides the binlog directory discovered from the MySQL
	// server variable log_bin_basename.
	BinlogDir string `json:"binlog_dir,omitempty"`
}

// Validate checks that required fields are present and returns an error if not.
func (c *Config) Validate() error {
	var errs []string

	if c.MySQL.User == "" {
		errs = append(errs, "mysql.user is required")
	}
	if c.MySQL.Password == "" {
		errs = append(errs, "mysql.password is required")
	}
	if c.MySQL.Database == "" {
		errs = append(errs, "mysql.database is required")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

// LoadConfig reads an encrypted config file, decrypts it with the given
// passphrase, and unmarshals the result into a Config struct.
func LoadConfig(path string, passphrase string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read file %s: %w", path, err)
	}

	plaintext, err := Decrypt(raw, passphrase)
	if err != nil {
		return nil, fmt.Errorf("config: decrypt %s: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(plaintext, &cfg); err != nil {
		return nil, fmt.Errorf("config: unmarshal %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// SaveConfig marshals the given Config, encrypts it with the passphrase, and
// writes the result to the given file path.
func SaveConfig(path string, passphrase string, cfg *Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("config: marshal: %w", err)
	}

	encrypted, err := Encrypt(data, passphrase)
	if err != nil {
		return fmt.Errorf("config: encrypt: %w", err)
	}

	if err := os.WriteFile(path, encrypted, 0600); err != nil {
		return fmt.Errorf("config: write %s: %w", path, err)
	}

	return nil
}

// ParseDSNToConnConfig parses a MySQL DSN string into a connector.ConnConfig.
// This is a convenience function for the CLI when --mysql-dsn is provided directly.
func ParseDSNToConnConfig(dsn string) (connector.ConnConfig, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return connector.ConnConfig{}, fmt.Errorf("config: parse DSN: %w", err)
	}

	// Parse addr (host:port)
	host := "127.0.0.1"
	port := 3306
	if cfg.Addr != "" {
		parts := strings.SplitN(cfg.Addr, ":", 2)
		host = parts[0]
		if len(parts) > 1 {
			fmt.Sscanf(parts[1], "%d", &port)
		}
	}

	// Copy params from the parsed DSN. The go-sql-driver/mysql ParseDSN
	// strips well-known params (tls, timeout, etc.) from cfg.Params and
	// stores them in typed fields instead. Add them back so the
	// connector.ConnConfig.Params map is complete.
	params := cfg.Params
	if params == nil {
		params = make(map[string]string)
	}
	if cfg.TLSConfig != "" {
		params["tls"] = cfg.TLSConfig
	}

	return connector.ConnConfig{
		Host:     host,
		Port:     port,
		User:     cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Params:   params,
	}, nil
}
