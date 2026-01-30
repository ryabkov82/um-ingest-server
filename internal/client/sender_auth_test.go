package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryabkov82/um-ingest-server/internal/ingest"
	"github.com/ryabkov82/um-ingest-server/internal/job"
)

func TestResolveDeliveryAuthPrefersPayload(t *testing.T) {
	jobAuth := &job.AuthConfig{
		Type: "basic",
		User: "payload_user",
		Pass: "payload_pass",
	}

	user, pass, fromPayload := ResolveDeliveryAuth(jobAuth, "env_user", "env_pass")

	if !fromPayload {
		t.Error("Expected fromPayload=true when job auth is provided")
	}
	if user != "payload_user" {
		t.Errorf("Expected user 'payload_user', got '%s'", user)
	}
	if pass != "payload_pass" {
		t.Errorf("Expected pass 'payload_pass', got '%s'", pass)
	}
}

func TestResolveDeliveryAuthFallsBackToEnv(t *testing.T) {
	user, pass, fromPayload := ResolveDeliveryAuth(nil, "env_user", "env_pass")

	if fromPayload {
		t.Error("Expected fromPayload=false when using env")
	}
	if user != "env_user" {
		t.Errorf("Expected user 'env_user', got '%s'", user)
	}
	if pass != "env_pass" {
		t.Errorf("Expected pass 'env_pass', got '%s'", pass)
	}
}

func TestResolveDeliveryAuthReturnsEmptyWhenNoCredentials(t *testing.T) {
	user, pass, fromPayload := ResolveDeliveryAuth(nil, "", "")

	if fromPayload {
		t.Error("Expected fromPayload=false when no credentials")
	}
	if user != "" {
		t.Errorf("Expected empty user, got '%s'", user)
	}
	if pass != "" {
		t.Errorf("Expected empty pass, got '%s'", pass)
	}
}

func TestEnvAuthUsedWhenNoPayload(t *testing.T) {
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create sender with env credentials but no job auth
	sender := NewSender(server.URL, false, 5, 0, 100, 1000, nil, "env_user", "env_pass", nil, false)

	batch := &ingest.Batch{
		PackageID: "test-package",
		BatchNo:   1,
		Register:  "TestRegister",
		Rows:      []map[string]interface{}{{"field1": "value1"}},
	}

	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)

	if err != nil {
		t.Errorf("SendBatch() error = %v", err)
	}

	if receivedAuth == "" {
		t.Error("Expected Authorization header, but it was not received")
	}

	// Authorization header should start with "Basic " and contain base64-encoded credentials
	if len(receivedAuth) < 6 || receivedAuth[:6] != "Basic " {
		t.Errorf("Expected Authorization header to start with 'Basic ', got: %s", receivedAuth)
	}
}

func TestPayloadAuthOverridesEnv(t *testing.T) {
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	jobAuth := &job.AuthConfig{
		Type: "basic",
		User: "payload_user",
		Pass: "payload_pass",
	}

	// Create sender with both job auth and env credentials
	sender := NewSender(server.URL, false, 5, 0, 100, 1000, jobAuth, "env_user", "env_pass", nil, false)

	batch := &ingest.Batch{
		PackageID: "test-package",
		BatchNo:   1,
		Register:  "TestRegister",
		Rows:      []map[string]interface{}{{"field1": "value1"}},
	}

	ctx := context.Background()
	err := sender.SendBatch(ctx, batch)

	if err != nil {
		t.Errorf("SendBatch() error = %v", err)
	}

	if receivedAuth == "" {
		t.Error("Expected Authorization header, but it was not received")
	}

	// Authorization header should start with "Basic " and contain base64-encoded credentials
	if len(receivedAuth) < 6 || receivedAuth[:6] != "Basic " {
		t.Errorf("Expected Authorization header to start with 'Basic ', got: %s", receivedAuth)
	}
}


