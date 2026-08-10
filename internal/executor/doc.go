// Package executor runs approved reverse-SQL plans against MySQL with
// checkpointing for resumable execution. Batches are wrapped in explicit
// transactions; on context cancellation the current batch is rolled back
// and the checkpoint reflects the last fully-committed batch.
package executor
