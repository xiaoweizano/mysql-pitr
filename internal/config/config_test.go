package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{
			MySQL: MySQLConfig{
				User:     "root",
				Password: "secret",
				Database: "mydb",
			},
		}
		require.NoError(t, cfg.Validate())
	})

	t.Run("missing user", func(t *testing.T) {
		cfg := &Config{
			MySQL: MySQLConfig{
				Password: "secret",
				Database: "mydb",
			},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("missing password", func(t *testing.T) {
		cfg := &Config{
			MySQL: MySQLConfig{
				User:     "root",
				Database: "mydb",
			},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("missing database", func(t *testing.T) {
		cfg := &Config{
			MySQL: MySQLConfig{
				User:     "root",
				Password: "secret",
			},
		}
		require.Error(t, cfg.Validate())
	})

	t.Run("all missing", func(t *testing.T) {
		cfg := &Config{}
		require.Error(t, cfg.Validate())
	})
}

func TestMySQLConfig_BuildConnConfig(t *testing.T) {
	t.Parallel()

	t.Run("full config", func(t *testing.T) {
		mc := MySQLConfig{
			Host:     "db.example.com",
			Port:     3307,
			User:     "app",
			Password: "p@ss",
			Database: "orders",
			Params:   map[string]string{"tls": "skip-verify"},
		}
		cc := mc.BuildConnConfig()
		assert.Equal(t, "db.example.com", cc.Host)
		assert.Equal(t, 3307, cc.Port)
		assert.Equal(t, "app", cc.User)
		assert.Equal(t, "p@ss", cc.Password)
		assert.Equal(t, "orders", cc.Database)
		assert.Equal(t, "skip-verify", cc.Params["tls"])
	})

	t.Run("defaults", func(t *testing.T) {
		mc := MySQLConfig{
			User:     "root",
			Password: "secret",
			Database: "test",
		}
		cc := mc.BuildConnConfig()
		assert.Equal(t, "127.0.0.1", cc.Host, "default host")
		assert.Equal(t, 3306, cc.Port, "default port")
	})
}

func TestMySQLConfig_BuildDSN(t *testing.T) {
	t.Parallel()

	mc := MySQLConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "secret",
		Database: "mydb",
	}
	dsn := mc.BuildDSN()
	assert.Contains(t, dsn, "root:secret@tcp(127.0.0.1:3306)/mydb")
}

func TestLoadSaveConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.conf")

	original := &Config{
		MySQL: MySQLConfig{
			Host:     "192.168.1.100",
			Port:     3306,
			User:     "replicator",
			Password: "supersecret!",
			Database: "production",
			Params: map[string]string{
				"tls": "skip-verify",
			},
		},
		Server: ServerConfig{
			URL:      "wss://platform.example.com/ws/agent",
			CertFile: "/etc/agent/client.pem",
		},
		DataDir: "/var/lib/agent/checkpoints",
	}

	passphrase := "test-passphrase-42"

	// Save
	err := SaveConfig(cfgPath, passphrase, original)
	require.NoError(t, err, "SaveConfig should succeed")

	// Verify the file exists and is not plain JSON
	data, err := os.ReadFile(cfgPath)
	require.NoError(t, err)
	require.NotContains(t, string(data), "supersecret", "file should be encrypted, not plaintext")
	require.NotContains(t, string(data), "replicator", "file should be encrypted, not plaintext")

	// Load
	loaded, err := LoadConfig(cfgPath, passphrase)
	require.NoError(t, err, "LoadConfig should succeed")
	require.NotNil(t, loaded)

	// Verify all fields
	assert.Equal(t, original.MySQL.Host, loaded.MySQL.Host)
	assert.Equal(t, original.MySQL.Port, loaded.MySQL.Port)
	assert.Equal(t, original.MySQL.User, loaded.MySQL.User)
	assert.Equal(t, original.MySQL.Password, loaded.MySQL.Password)
	assert.Equal(t, original.MySQL.Database, loaded.MySQL.Database)
	assert.Equal(t, original.MySQL.Params, loaded.MySQL.Params)
	assert.Equal(t, original.Server.URL, loaded.Server.URL)
	assert.Equal(t, original.Server.CertFile, loaded.Server.CertFile)
	assert.Equal(t, original.DataDir, loaded.DataDir)
}

func TestArchiveConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	original := &Config{
		MySQL: MySQLConfig{User: "root", Password: "secret", Database: "mydb"},
		Archive: &ArchiveConfig{
			Dir:           "/var/lib/mysql-pitr/archive",
			ServerID:      1001,
			RetentionDays: 7,
		},
	}

	// 序列化断言：archive 段带 dir/server_id/retention_days 键。
	data, err := json.Marshal(original)
	require.NoError(t, err)
	var raw map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &raw))
	arc, ok := raw["archive"].(map[string]interface{})
	require.True(t, ok, "config JSON must include an archive section")
	assert.Equal(t, "/var/lib/mysql-pitr/archive", arc["dir"])
	assert.Equal(t, float64(1001), arc["server_id"])
	assert.Equal(t, float64(7), arc["retention_days"])

	// 加密存取往返。
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.conf")
	require.NoError(t, SaveConfig(cfgPath, "passphrase", original))
	loaded, err := LoadConfig(cfgPath, "passphrase")
	require.NoError(t, err)
	require.NotNil(t, loaded.Archive, "archive section must survive the roundtrip")
	assert.Equal(t, *original.Archive, *loaded.Archive)
}

func TestArchiveConfig_ZeroOmitted(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(&Config{
		MySQL: MySQLConfig{User: "u", Password: "p", Database: "d"},
	})
	require.NoError(t, err)
	assert.NotContains(t, string(data), "archive", "zero archive config must be omitted")
}

func TestLoadConfig_WrongPassphrase(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.conf")

	cfg := &Config{
		MySQL: MySQLConfig{
			User:     "root",
			Password: "secret",
			Database: "db",
		},
	}

	err := SaveConfig(cfgPath, "correct-passphrase", cfg)
	require.NoError(t, err)

	_, err = LoadConfig(cfgPath, "wrong-passphrase")
	require.Error(t, err, "loading with wrong passphrase should fail")
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := LoadConfig("/nonexistent/path/config.conf", "pass")
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist, "should return a not-exist error")
}

func TestParseDSNToConnConfig_FullDSN(t *testing.T) {
	t.Parallel()

	cc, err := ParseDSNToConnConfig("user:pass@tcp(db.example.com:3307)/mydb?tls=skip-verify&timeout=5s")
	require.NoError(t, err)
	assert.Equal(t, "db.example.com", cc.Host)
	assert.Equal(t, 3307, cc.Port)
	assert.Equal(t, "user", cc.User)
	assert.Equal(t, "pass", cc.Password)
	assert.Equal(t, "mydb", cc.Database)
	assert.Equal(t, "skip-verify", cc.Params["tls"])
	// timeout may be set in params but the DSN parsing handles it specially
}

func TestParseDSNToConnConfig_SimpleDSN(t *testing.T) {
	t.Parallel()

	cc, err := ParseDSNToConnConfig("root:secret@/mydb")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", cc.Host, "default host when addr empty")
	assert.Equal(t, 3306, cc.Port, "default port")
	assert.Equal(t, "root", cc.User)
	assert.Equal(t, "secret", cc.Password)
	assert.Equal(t, "mydb", cc.Database)
}

func TestParseDSNToConnConfig_Invalid(t *testing.T) {
	t.Parallel()

	_, err := ParseDSNToConnConfig("not-a-valid-dsn!")
	require.Error(t, err)
}
