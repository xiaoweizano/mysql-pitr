// Package binlog reads MySQL binlog files using go-mysql and aggregates
// events into Transactions bounded by XID/GTID/COMMIT events.
//
// The Scanner interface exposes a Next()-style iterator; callers drive the
// scan loop and stop on io.EOF or context cancellation.
package binlog
