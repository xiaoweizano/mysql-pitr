package ws

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestScanRequestRoundTrip(t *testing.T) {
	orig := ScanRequest{
		Filter: ScanFilter{
			Tables: []TableRefJSON{
				{Schema: "app", Table: "users"},
				{Schema: "app", Table: "orders"},
			},
			TimeStart:     "2026-08-01T00:00:00+08:00",
			TimeEnd:       "2026-08-02T00:00:00Z",
			GTIDSet:       "uuid:1-100",
			StartFile:     "mysql-bin.000015",
			StartPos:      156,
			EndFile:       "mysql-bin.000018",
			EndPos:        820,
			MaxRowsPerTx:  500,
			SelectedTxIDs: []string{"00000001-0000-0000-0000-000000000001:1", "00000001-0000-0000-0000-000000000001:2"},
		},
		Mode:       "sql",
		MaxPreview: 100,
	}

	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got ScanRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(got, orig) {
		t.Fatalf("roundtrip mismatch:\n got=%+v\nwant=%+v", got, orig)
	}
}

func TestScanRequestEmptySelectedTxIDsZeroValue(t *testing.T) {
	// An absent selectedTxIds field must unmarshal to the zero value (nil),
	// and marshaling a zero-valued filter must omit it entirely.
	req := ScanRequest{Mode: "meta"}

	data, err := json.Marshal(&req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("marshal produced empty payload")
	}

	var got ScanRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Filter.SelectedTxIDs != nil {
		t.Fatalf("expected nil SelectedTxIDs after roundtrip, got %v", got.Filter.SelectedTxIDs)
	}
	if len(got.Filter.SelectedTxIDs) != 0 {
		t.Fatalf("expected empty SelectedTxIDs, got %v", got.Filter.SelectedTxIDs)
	}
}

func TestScanRequestTimeStringsPreserved(t *testing.T) {
	// TimeStart/TimeEnd are opaque RFC3339 strings; they must roundtrip
	// byte-for-byte with no normalization or time parsing.
	orig := ScanRequest{
		Mode: "selected",
		Filter: ScanFilter{
			TimeStart:     "2026-08-10T12:34:56.789+08:00",
			TimeEnd:       "2026-08-10T13:00:00Z",
			SelectedTxIDs: []string{"00000001-0000-0000-0000-000000000001:7"},
		},
	}

	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got ScanRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Filter.TimeStart != orig.Filter.TimeStart {
		t.Fatalf("TimeStart not preserved: got %q want %q", got.Filter.TimeStart, orig.Filter.TimeStart)
	}
	if got.Filter.TimeEnd != orig.Filter.TimeEnd {
		t.Fatalf("TimeEnd not preserved: got %q want %q", got.Filter.TimeEnd, orig.Filter.TimeEnd)
	}
}

func TestExecuteRequestRoundTrip(t *testing.T) {
	orig := ExecuteRequest{
		OperationID: "op-42",
		Statements: []StatementWire{
			{SQL: "INSERT INTO users (id, name) VALUES (1, 'a')", TxID: "00000001-0000-0000-0000-000000000001:1", TxOrder: 0},
			{SQL: "UPDATE users SET name='b' WHERE id=1", TxID: "00000001-0000-0000-0000-000000000001:1", TxOrder: 1, Warnings: []string{"deprecated syntax"}},
		},
		BatchSize: 50,
	}

	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got ExecuteRequest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(got, orig) {
		t.Fatalf("roundtrip mismatch:\n got=%+v\nwant=%+v", got, orig)
	}
}

func TestStreamEventRoundTrip(t *testing.T) {
	cases := []StreamEvent{
		{
			ID:   "scan-1",
			Kind: EvTxMeta,
			Data: json.RawMessage(`{"txId":"00000001-0000-0000-0000-000000000001:1","rows":3}`),
		},
		{
			ID:   "scan-1",
			Kind: EvSQL,
			Data: json.RawMessage(`{"sql":"SELECT * FROM users","txId":"00000001-0000-0000-0000-000000000001:1"}`),
		},
		{
			ID:   "scan-1",
			Kind: EvScanDone,
			Data: json.RawMessage(`{"tablesScanned":2,"totalTxs":15}`),
		},
		{
			ID:   "exec-9",
			Kind: EvProgress,
			Data: json.RawMessage(`{"done":5,"total":10}`),
		},
		{
			ID:   "exec-9",
			Kind: EvOpDone,
			Data: json.RawMessage(`{"operationId":"op-42","status":"ok"}`),
		},
		{
			ID:   "exec-9",
			Kind: EvOpError,
			Data: json.RawMessage(`{"operationId":"op-42","error":"timeout"}`),
		},
	}

	for i, orig := range cases {
		data, err := json.Marshal(&orig)
		if err != nil {
			t.Fatalf("case %d marshal: %v", i, err)
		}

		var got StreamEvent
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("case %d unmarshal: %v", i, err)
		}

		if got.ID != orig.ID || got.Kind != orig.Kind {
			t.Fatalf("case %d header mismatch: got %+v want %+v", i, got, orig)
		}
		if string(got.Data) != string(orig.Data) {
			t.Fatalf("case %d Data not preserved:\n got=%s\nwant=%s", i, string(got.Data), string(orig.Data))
		}
	}
}

func TestCommandTypeConstants(t *testing.T) {
	expected := map[string]string{
		"CmdScan":          CmdScan,
		"CmdExecute":       CmdExecute,
		"CmdResume":        CmdResume,
		"CmdCancel":        CmdCancel,
		"CmdArchiveStatus": CmdArchiveStatus,
	}
	wantValues := map[string]string{
		"CmdScan":          "scan",
		"CmdExecute":       "execute",
		"CmdResume":        "resume",
		"CmdCancel":        "cancel",
		"CmdArchiveStatus": "archive_status",
	}
	for name, got := range expected {
		if want := wantValues[name]; got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestCmdStreamEventConstant(t *testing.T) {
	if CmdStreamEvent != "stream_event" {
		t.Fatalf("CmdStreamEvent = %q, want %q", CmdStreamEvent, "stream_event")
	}
}

func TestStreamEventKindConstants(t *testing.T) {
	expected := map[string]string{
		"EvTxMeta":   EvTxMeta,
		"EvSQL":      EvSQL,
		"EvScanDone": EvScanDone,
		"EvProgress": EvProgress,
		"EvOpDone":   EvOpDone,
		"EvOpError":  EvOpError,
		"EvOpPaused": EvOpPaused,
	}
	wantValues := map[string]string{
		"EvTxMeta":   "tx_meta",
		"EvSQL":      "sql",
		"EvScanDone": "scan_done",
		"EvProgress": "progress",
		"EvOpDone":   "op_done",
		"EvOpError":  "op_error",
		"EvOpPaused": "op_paused",
	}
	for name, got := range expected {
		if want := wantValues[name]; got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
}
