// Package reverse generates reverse (undo) SQL from binlog Transactions.
// Pure logic — no IO, no side effects. Given the same Transaction and
// schema map, Generate always returns the same Statements.
package reverse
