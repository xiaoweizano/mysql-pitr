package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-shan/mysql-pitr/internal/binlog"
	"github.com/a-shan/mysql-pitr/internal/config"
	"github.com/a-shan/mysql-pitr/internal/connector"
	"github.com/a-shan/mysql-pitr/internal/executor"
	"github.com/a-shan/mysql-pitr/internal/reverse"
)

// FlashbackOptions holds all input parameters for a flashback operation.
type FlashbackOptions struct {
	// Connector is an optional pre-configured connector. When nil, one is created
	// from DSN (or config file). Inject a mock for testing.
	Connector connector.Connector

	// CLI flags
	DSN          string
	TargetTable  string
	RecoveryTime string
	OutputFile   string
	DryRun       bool
	BatchSize    int
	ConfigFile   string
	Passphrase   string
}

// newBinlogScanner is a package-level seam so tests can substitute a fake
// scanner without a live binlog directory.
var newBinlogScanner = func(sf binlog.SchemaFetcher, opts ...binlog.Option) binlog.Scanner {
	return binlog.NewScanner(sf, opts...)
}

// RunFlashback executes the flashback workflow:
//  1. Connect to MySQL (from DSN or encrypted config)
//  2. Run preflight checks
//  3. Locate the binlog directory
//  4. Use binlog.Scanner to enumerate matching transactions
//  5. Use reverse.Generate to produce reverse SQL
//  6. If --dry-run: print SQL to stdout (warnings to stderr)
//  7. If --output: write SQL to file
//  8. If neither flag: execute against MySQL via executor
func RunFlashback(ctx context.Context, opts FlashbackOptions) error {
	// ---- Parse recovery time ----
	recoveryTime, err := time.Parse(time.RFC3339, opts.RecoveryTime)
	if err != nil {
		return fmt.Errorf("flashback: parse recovery-time %q: %w", opts.RecoveryTime, err)
	}

	// ---- Connect ----
	conn := opts.Connector
	var connCfg connector.ConnConfig
	if conn == nil {
		connCfg, err = resolveConnConfig(opts)
		if err != nil {
			return fmt.Errorf("flashback: resolve config: %w", err)
		}
		conn = connector.NewMySQLConnector()
		if err := conn.Connect(connCfg); err != nil {
			return fmt.Errorf("flashback: connect: %w", err)
		}
		log.Printf("connected to MySQL at %s:%d (database: %s)", connCfg.Host, connCfg.Port, connCfg.Database)
	}
	defer conn.Close()

	// ---- Preflight ----
	preflightRes, err := conn.Preflight(ctx)
	if err != nil {
		return fmt.Errorf("flashback: preflight: %w", err)
	}
	if err := preflightRes.EnsureOK(); err != nil {
		return fmt.Errorf("flashback: preflight: %w", err)
	}
	log.Printf("preflight: %s (MySQL %s)", preflightRes.Status, preflightRes.Version)

	// ---- Locate binlog directory ----
	binlogDir, err := conn.GetBinlogDir(ctx)
	if err != nil {
		return fmt.Errorf("flashback: locate binlog dir: %w", err)
	}
	log.Printf("MySQL binlog directory: %s", binlogDir)

	// ---- Parse target table into schema.table ----
	schema, table, err := splitTableRef(opts.TargetTable)
	if err != nil {
		return err
	}

	// ---- Scan binlogs for matching transactions ----
	scanner := newBinlogScanner(conn, binlog.WithMaxRowsPerTx(1_000_000))
	filter := binlog.Filter{
		BinlogDir: binlogDir,
		Tables:    []binlog.TableRef{{Schema: schema, Table: table}},
		TimeRange: &binlog.TimeRange{End: recoveryTime},
	}
	if err := scanner.Scan(ctx, filter); err != nil {
		return fmt.Errorf("flashback: scan: %w", err)
	}

	// ---- Collect transactions + schema map (cached per schema.table) ----
	schemaMap := map[string]binlog.TableSchema{}
	var allTx []*binlog.Transaction
	for {
		tx, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("flashback: scan next: %w", err)
		}
		for _, rc := range tx.Statements {
			key := rc.Schema + "." + rc.Table
			if _, ok := schemaMap[key]; !ok {
				sch, err := conn.FetchSchema(ctx, rc.Schema, rc.Table)
				if err != nil {
					log.Printf("warn: fetch schema for %s: %v", key, err)
					continue
				}
				schemaMap[key] = sch
			}
		}
		allTx = append(allTx, tx)
	}
	scanner.Close()
	if len(allTx) == 0 {
		return fmt.Errorf("flashback: no transactions found for table %q before %s", opts.TargetTable, opts.RecoveryTime)
	}
	log.Printf("scanned %d transactions", len(allTx))

	// ---- Generate reverse SQL for all transactions ----
	var allStmts []reverse.Statement
	for _, tx := range allTx {
		stmts, err := reverse.Generate(tx, schemaMap, reverse.Options{})
		if err != nil {
			return fmt.Errorf("flashback: generate for tx %s: %w", tx.TxID, err)
		}
		allStmts = append(allStmts, stmts...)
	}
	log.Printf("generated %d reverse statement(s)", len(allStmts))

	// ---- Output / execute ----
	if opts.DryRun {
		// Print SQL to stdout; statements that could not be generated to stderr.
		for _, s := range allStmts {
			if s.SQL == "" {
				fmt.Fprintf(os.Stderr, "-- WARN tx=%s order=%d: %v\n", s.TxID, s.TxOrder, s.Warnings)
				continue
			}
			fmt.Println(s.SQL + ";")
		}
		return nil
	}

	if opts.OutputFile != "" {
		sqls := make([]string, 0, len(allStmts))
		for _, s := range allStmts {
			if s.SQL != "" {
				sqls = append(sqls, s.SQL+";")
			}
		}
		if err := writeSQLToFile(opts.OutputFile, sqls); err != nil {
			return fmt.Errorf("flashback: write output: %w", err)
		}
		log.Printf("wrote %d statement(s) to %s", len(sqls), opts.OutputFile)
		return nil
	}

	// ---- Execute via executor with an in-memory checkpoint store ----
	plan := executor.Plan{
		OperationID: "cli-" + time.Now().UTC().Format("20060102T150405Z"),
		Statements:  allStmts,
		DSN:         connCfg.DSN(),
		BatchSize:   opts.BatchSize,
	}
	dbFactory := func(executor.Plan) (executor.DB, error) {
		if db := conn.AsDB(); db != nil {
			return db, nil
		}
		return nil, fmt.Errorf("connector does not expose a database handle")
	}
	store := executor.NewInMemoryCheckpointStore()
	ex := executor.NewExecutor(dbFactory, store)

	report, err := ex.Run(ctx, plan, func(p executor.Progress) {
		log.Printf("progress: %d/%d (errors=%d)", p.Done, p.Total, len(p.Errors))
	})
	if err != nil {
		return fmt.Errorf("flashback: execute: %w", err)
	}
	log.Printf("done: %d/%d, errors=%d, paused=%v", report.Done, report.Total, len(report.Errors), report.Paused)
	for _, e := range report.Errors {
		log.Printf("  error: %s", e.Err)
	}
	return nil
}

// resolveConnConfig determines the connection configuration from either the
// --config file, the --mysql-dsn flag, or the built-in config.
func resolveConnConfig(opts FlashbackOptions) (connector.ConnConfig, error) {
	// If --config is provided, load from encrypted config file.
	if opts.ConfigFile != "" {
		cfg, err := config.LoadConfig(opts.ConfigFile, opts.Passphrase)
		if err != nil {
			return connector.ConnConfig{}, fmt.Errorf("load config: %w", err)
		}
		return cfg.MySQL.BuildConnConfig(), nil
	}

	// If --mysql-dsn is provided, parse it.
	if opts.DSN != "" {
		return config.ParseDSNToConnConfig(opts.DSN)
	}

	return connector.ConnConfig{}, fmt.Errorf("either --mysql-dsn or --config must be provided")
}

// splitTableRef splits a "schema.table" target reference into its parts.
func splitTableRef(s string) (schema, table string, err error) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("flashback: target table %q must be schema.table", s)
	}
	return parts[0], parts[1], nil
}

// writeSQLToFile writes the given SQL statements to the given file path.
func writeSQLToFile(path string, sqls []string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer f.Close()

	for _, s := range sqls {
		if _, err := fmt.Fprintln(f, s); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}

	return f.Close()
}

// NewFlashbackCommand creates the `agent flashback` cobra command.
func NewFlashbackCommand() *cobra.Command {
	opts := FlashbackOptions{}

	cmd := &cobra.Command{
		Use:   "flashback",
		Short: "Perform point-in-time recovery via binlog flashback",
		Long: `Perform point-in-time recovery by parsing MySQL binary logs and
generating reverse SQL statements to undo changes made before a
specified recovery time.

The workflow:
  1. Connects to MySQL (via DSN or encrypted config file)
  2. Runs preflight readiness checks
  3. Scans binlog files for the target table before the recovery time
  4. Generates reverse SQL (INSERT→DELETE, DELETE→INSERT, UPDATE→SET old values)
  5. Either dry-runs, writes to a file, or executes against MySQL
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate required flags.
			if opts.ConfigFile == "" && opts.DSN == "" {
				return fmt.Errorf("either --mysql-dsn or --config is required")
			}
			if opts.RecoveryTime == "" {
				return fmt.Errorf("--recovery-time is required")
			}
			if opts.TargetTable == "" {
				return fmt.Errorf("--target-table is required")
			}
			if opts.ConfigFile != "" && opts.Passphrase == "" {
				return fmt.Errorf("--passphrase is required when using --config")
			}

			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			return RunFlashback(ctx, opts)
		},
		SilenceUsage: true,
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.DSN, "mysql-dsn", "", "MySQL DSN (e.g. user:pass@tcp(host:3306)/db)")
	flags.StringVar(&opts.TargetTable, "target-table", "", "Target table in schema.table format (required)")
	flags.StringVar(&opts.RecoveryTime, "recovery-time", "", "ISO8601 timestamp for recovery point (required)")
	flags.StringVar(&opts.OutputFile, "output", "", "File path for reverse SQL output")
	flags.BoolVar(&opts.DryRun, "dry-run", false, "Print reverse SQL without executing")
	flags.IntVar(&opts.BatchSize, "batch-size", 1000, "Batch size for execution")
	flags.StringVar(&opts.ConfigFile, "config", "", "Encrypted config file path")
	flags.StringVar(&opts.Passphrase, "passphrase", "", "Passphrase for config decryption")

	return cmd
}
