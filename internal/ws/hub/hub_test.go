package hub

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/a-shan/mysql-pitr/internal/ws"
	gorilla "github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testBundle holds PEM-encoded test certificates.
type testBundle struct {
	CACert     string
	CAKey      string
	ClientCert string
	ClientKey  string
}

// generateTestCerts creates a self-signed CA and a client cert with the
// given CommonName, returning PEM-encoded bytes.
func generateTestCerts(t *testing.T, cn string) *testBundle {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caTmpl, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}

	return &testBundle{
		CACert:     string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		CAKey:      string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: marshalECKey(caKey)})),
		ClientCert: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})),
		ClientKey:  string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: marshalECKey(clientKey)})),
	}
}

func marshalECKey(key *ecdsa.PrivateKey) []byte {
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		panic(err)
	}
	return b
}

// newTestTLSServer creates an HTTPS server with mTLS and registers the hub
// to handle the /ws/agent endpoint. Returns the base URL.
func newTestTLSServer(t *testing.T, hub *Hub, bundle *testBundle) string {
	t.Helper()

	caBlock, _ := pem.Decode([]byte(bundle.CACert))
	if caBlock == nil {
		t.Fatal("decode CA PEM")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	serverCert, err := tls.X509KeyPair([]byte(bundle.ClientCert), []byte(bundle.ClientKey))
	if err != nil {
		t.Fatalf("load server keypair: %v", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/agent", func(w http.ResponseWriter, r *http.Request) {
		upgrader := gorilla.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade: %v", err)
			return
		}
		hub.HandleConnection(conn, r)
	})

	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatalf("TLS listen: %v", err)
	}

	srv := &http.Server{Handler: mux}
	t.Cleanup(func() {
		_ = srv.Close()
		_ = listener.Close()
	})

	go func() { _ = srv.Serve(listener) }()

	addr := listener.Addr().(*net.TCPAddr)
	return fmt.Sprintf("wss://127.0.0.1:%d/ws/agent", addr.Port)
}

// dialTestAgent connects a test WebSocket client to the test server.
func dialTestAgent(t *testing.T, serverURL string, bundle *testBundle, agentID string) *gorilla.Conn {
	t.Helper()

	clientCert, err := tls.X509KeyPair([]byte(bundle.ClientCert), []byte(bundle.ClientKey))
	if err != nil {
		t.Fatalf("load client keypair: %v", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		InsecureSkipVerify: true, // test env
		MinVersion:   tls.VersionTLS12,
	}

	dialer := &gorilla.Dialer{
		TLSClientConfig:  tlsCfg,
		HandshakeTimeout: 5 * time.Second,
	}

	conn, _, err := dialer.Dial(serverURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestNewHub(t *testing.T) {
	h := NewHub("")
	if h == nil {
		t.Fatal("NewHub returned nil")
	}
}

func TestHandleConnectionRegistersAgent(t *testing.T) {
	bundle := generateTestCerts(t, "agent-1")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "agent-1")
	defer conn.Close()

	// Give the hub time to process the connection.
	time.Sleep(200 * time.Millisecond)

	agents := hub.GetConnectedAgents()
	if len(agents) != 1 {
		t.Fatalf("expected 1 connected agent, got %d", len(agents))
	}
	if agents[0].ID != "agent-1" {
		t.Errorf("expected agent ID 'agent-1', got %q", agents[0].ID)
	}
	if agents[0].Status != "online" {
		t.Errorf("expected status 'online', got %q", agents[0].Status)
	}
}

func TestRejectDuplicateAgent(t *testing.T) {
	bundle := generateTestCerts(t, "dup-agent")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, bundle)
	conn1 := dialTestAgent(t, serverURL, bundle, "dup-agent")
	defer conn1.Close()

	time.Sleep(200 * time.Millisecond)

	// Second connection with same agent ID should be rejected.
	conn2 := dialTestAgent(t, serverURL, bundle, "dup-agent")
	defer conn2.Close()

	time.Sleep(200 * time.Millisecond)

	agents := hub.GetConnectedAgents()
	if len(agents) != 1 {
		t.Fatalf("expected 1 connected agent after duplicate reject, got %d", len(agents))
	}
}

func TestExtractAgentIDMissingTLS(t *testing.T) {
	id := extractAgentID(&http.Request{})
	if id != "" {
		t.Errorf("expected empty, got %q", id)
	}
}

func TestGetConnectedAgentsEmpty(t *testing.T) {
	hub := NewHub("")
	agents := hub.GetConnectedAgents()
	if agents == nil {
		t.Fatal("expected non-nil slice")
	}
	if len(agents) != 0 {
		t.Errorf("expected 0, got %d", len(agents))
	}
}

func TestSendToAgent(t *testing.T) {
	bundle := generateTestCerts(t, "agent-cmd")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, bundle)

	// Agent handler reads commands and responds.
	conn := dialTestAgent(t, serverURL, bundle, "agent-cmd")
	defer conn.Close()

	// Send a command via the hub and verify the agent receives it and the
	// response is correlated back.
	cmd := ws.Command{
		Cmd:  "cmd-1",
		Type: "test-command",
	}
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg2, err := conn.ReadMessage()
		if err != nil {
			t.Logf("agent read command: %v", err)
			return
		}
		var cmd2r ws.Command
		if err := json.Unmarshal(msg2, &cmd2r); err != nil {
			t.Logf("unmarshal cmd: %v", err)
			return
		}
		resp2 := ws.Response{Cmd: cmd2r.Cmd, Status: ws.StatusOK, Result: "cmd1-done"}
		d, _ := json.Marshal(resp2)
		_ = conn.WriteMessage(gorilla.TextMessage, d)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gotResp, err := hub.SendToAgent(ctx, "agent-cmd", cmd)
	if err != nil {
		t.Fatalf("SendToAgent: %v", err)
	}
	if gotResp.Status != ws.StatusOK {
		t.Errorf("expected ok, got %q", gotResp.Status)
	}
	if gotResp.Result != "cmd1-done" {
		t.Errorf("expected result 'cmd1-done', got %v", gotResp.Result)
	}
}

func TestSendToAgentNotConnected(t *testing.T) {
	hub := NewHub("")
	_, err := hub.SendToAgent(context.Background(), "unknown", ws.Command{
		Cmd: "test", Type: "test",
	})
	if err == nil {
		t.Fatal("expected error for unknown agent")
	}
}

func TestPITRProgressDelivery(t *testing.T) {
	bundle := generateTestCerts(t, "progress-agent")
	hub := NewHub("")

	received := make(chan ws.Command, 1)
	hub.SetProgressHandler(func(agentID string, cmd ws.Command) {
		received <- cmd
	})

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "progress-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	// The agent pushes a pitr_progress notification.
	progress := ws.Command{
		Cmd:  "progress-op_1",
		Type: ws.CmdPITRProgress,
		Params: map[string]interface{}{
			"operationId":     "op_1",
			"batchesComplete": 3,
			"batchesTotal":    10,
		},
	}
	data, err := json.Marshal(progress)
	if err != nil {
		t.Fatalf("marshal progress: %v", err)
	}
	if err := conn.WriteMessage(gorilla.TextMessage, data); err != nil {
		t.Fatalf("write progress: %v", err)
	}

	select {
	case got := <-received:
		if got.Type != ws.CmdPITRProgress {
			t.Errorf("expected pitr_progress, got %q", got.Type)
		}
		if got.Params["operationId"] != "op_1" {
			t.Errorf("expected operationId op_1, got %v", got.Params["operationId"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("progress handler was not invoked")
	}
}

// TestStreamEventDelivery verifies that an agent-pushed stream_event envelope
// is routed to the registered stream event handler with agentID and the raw
// command (params id/kind/data preserved as sent).
func TestStreamEventDelivery(t *testing.T) {
	bundle := generateTestCerts(t, "stream-agent")
	hub := NewHub("")

	type delivery struct {
		agentID string
		cmd     ws.Command
	}
	received := make(chan delivery, 1)
	hub.SetStreamEventHandler(func(agentID string, cmd ws.Command) {
		received <- delivery{agentID: agentID, cmd: cmd}
	})

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "stream-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	// The agent pushes a stream_event envelope. On the wire, params carry the
	// id/kind/data of a ws.StreamEvent; data is raw JSON that round-trips to a
	// generic decoded value.
	ev := ws.StreamEvent{
		ID:   "scan-1",
		Kind: ws.EvTxMeta,
		Data: json.RawMessage(`{"txId":"00000001-0000-0000-0000-000000000001:1","rows":3}`),
	}
	var dataVal interface{}
	if err := json.Unmarshal(ev.Data, &dataVal); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}
	envelope := ws.Command{
		Cmd:  ev.ID,
		Type: ws.CmdStreamEvent,
		Params: map[string]interface{}{
			"id":   ev.ID,
			"kind": ev.Kind,
			"data": dataVal,
		},
	}
	wire, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	if err := conn.WriteMessage(gorilla.TextMessage, wire); err != nil {
		t.Fatalf("write stream event: %v", err)
	}

	select {
	case got := <-received:
		if got.agentID != "stream-agent" {
			t.Errorf("expected agentID stream-agent, got %q", got.agentID)
		}
		if got.cmd.Type != ws.CmdStreamEvent {
			t.Errorf("expected type stream_event, got %q", got.cmd.Type)
		}
		if got.cmd.Cmd != ev.ID {
			t.Errorf("expected cmd %q, got %q", ev.ID, got.cmd.Cmd)
		}
		if got.cmd.Params["id"] != ev.ID {
			t.Errorf("expected params.id %q, got %v", ev.ID, got.cmd.Params["id"])
		}
		if got.cmd.Params["kind"] != ev.Kind {
			t.Errorf("expected params.kind %q, got %v", ev.Kind, got.cmd.Params["kind"])
		}
		if !reflect.DeepEqual(got.cmd.Params["data"], dataVal) {
			t.Errorf("params.data not preserved intact:\n got=%v\nwant=%v", got.cmd.Params["data"], dataVal)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream event handler was not invoked")
	}
}

func TestSendToAgentTimeout(t *testing.T) {
	bundle := generateTestCerts(t, "timeout-agent")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "timeout-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	// No agent goroutine responds — the command must time out.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := hub.SendToAgent(ctx, "timeout-agent", ws.Command{
		Cmd: "slow", Type: "never-answered",
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestSendToAgentDisconnectDrainsPending(t *testing.T) {
	bundle := generateTestCerts(t, "drop-agent")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "drop-agent")

	time.Sleep(200 * time.Millisecond)

	// Send a command, then kill the connection while it is in flight. The
	// pending channel is drained so SendToAgent must return an error instead
	// of hanging.
	errCh := make(chan error, 1)
	go func() {
		_, err := hub.SendToAgent(context.Background(), "drop-agent", ws.Command{
			Cmd: "in-flight", Type: "never-answered",
		})
		errCh <- err
	}()

	time.Sleep(300 * time.Millisecond)
	_ = conn.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after agent disconnect")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SendToAgent hung after agent disconnect")
	}
}

func TestHandleConnectionRevokedCert(t *testing.T) {
	bundle := generateTestCerts(t, "revoked-agent")
	hub := NewHub("")

	// Create a CSRHandler that reports this cert as revoked.
	hub.SetCSRHandler(&mockCSRHandler{
		isRevokedFn: func(serial string) bool {
			return serial == "2" // our test client serial
		},
	})

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "revoked-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	agents := hub.GetConnectedAgents()
	if len(agents) != 0 {
		t.Errorf("expected 0 agents (revoked), got %d", len(agents))
	}
}

func TestHandleConnectionCertRenewal(t *testing.T) {
	bundle := generateTestCerts(t, "renew-agent")
	hub := NewHub("")

	hub.SetCSRHandler(&mockCSRHandler{
		signCSRFn: func(csrPEM []byte, agentID string) ([]byte, error) {
			return []byte("signed-cert-data"), nil
		},
	})

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "renew-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	// Send a cert_renewal command.
	renewCmd := ws.Command{
		Cmd:  "renew-1",
		Type: ws.CmdCertRenewal,
		Params: map[string]interface{}{
			"csr": "fake-csr-pem",
		},
	}
	data, _ := json.Marshal(renewCmd)
	_ = conn.WriteMessage(gorilla.TextMessage, data)

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read renewal response: %v", err)
	}
	var resp ws.Response
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Status != ws.StatusOK {
		t.Errorf("expected ok, got %q: %s", resp.Status, resp.Error)
	}
}

func TestHandleConnectionCertRenewalNoCA(t *testing.T) {
	bundle := generateTestCerts(t, "no-ca-agent")
	hub := NewHub("")
	// No CSRHandler set.

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "no-ca-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	renewCmd := ws.Command{
		Cmd:  "renew-2",
		Type: ws.CmdCertRenewal,
		Params: map[string]interface{}{
			"csr": "fake-csr",
		},
	}
	data, _ := json.Marshal(renewCmd)
	_ = conn.WriteMessage(gorilla.TextMessage, data)

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp ws.Response
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Status != ws.StatusError {
		t.Errorf("expected error, got %q", resp.Status)
	}
}

func TestClose(t *testing.T) {
	bundle := generateTestCerts(t, "close-agent")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, bundle)
	conn := dialTestAgent(t, serverURL, bundle, "close-agent")
	defer conn.Close()

	time.Sleep(200 * time.Millisecond)

	if err := hub.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	agents := hub.GetConnectedAgents()
	if len(agents) != 0 {
		t.Errorf("expected 0 agents after close, got %d", len(agents))
	}
}

func TestCloseIdempotent(t *testing.T) {
	hub := NewHub("")
	if err := hub.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := hub.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestHubConcurrency(t *testing.T) {
	// All agents dial with client certs signed by the same CA; the server
	// trust anchor comes from the first bundle.
	caBundle := generateTestCerts(t, "concurrent-ca")
	hub := NewHub("")

	serverURL := newTestTLSServer(t, hub, caBundle)

	const numAgents = 5
	var conns []*gorilla.Conn

	for i := 0; i < numAgents; i++ {
		b := newClientBundle(t, caBundle, fmt.Sprintf("concurrent-%d", i))
		c := dialTestAgent(t, serverURL, b, fmt.Sprintf("concurrent-%d", i))
		conns = append(conns, c)
	}

	time.Sleep(500 * time.Millisecond)

	agents := hub.GetConnectedAgents()
	if len(agents) != numAgents {
		t.Fatalf("expected %d agents, got %d", numAgents, len(agents))
	}

	// Cleanup.
	for _, c := range conns {
		c.Close()
	}
	_ = hub.Close()
}

// newClientBundle creates a client certificate signed by the CA from an
// existing test bundle, so multiple distinct agent identities can share one
// trust anchor.
func newClientBundle(t *testing.T, caBundle *testBundle, cn string) *testBundle {
	t.Helper()

	caBlock, _ := pem.Decode([]byte(caBundle.CACert))
	if caBlock == nil {
		t.Fatal("decode CA cert PEM")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	caKeyBlock, _ := pem.Decode([]byte(caBundle.CAKey))
	if caKeyBlock == nil {
		t.Fatal("decode CA key PEM")
	}
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA key: %v", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano() + int64(len(cn))),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}

	return &testBundle{
		CACert:     caBundle.CACert,
		CAKey:      caBundle.CAKey,
		ClientCert: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})),
		ClientKey:  string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: marshalECKey(clientKey)})),
	}
}
