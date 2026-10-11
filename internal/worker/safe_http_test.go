package worker

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestSafeHTTPClientBlocksLoopbackWithoutAllowlist(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newSafeHTTPClient()
	resp, err := client.Get(server.URL)
	if resp != nil {
		resp.Body.Close()
	}

	if err == nil {
		t.Fatal("expected loopback request to be blocked")
	}
}

func TestSafeHTTPClientAllowsAllowlistedLoopback(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "127.0.0.1")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := newSafeHTTPClient()
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("expected allowlisted loopback request to succeed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestSafeHTTPClientBlocksPrivateIP(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "")

	client := newSafeHTTPClient()
	resp, err := client.Get("http://192.168.1.1:8080/")
	if resp != nil {
		resp.Body.Close()
	}

	if err == nil {
		t.Fatal("expected private IP request to be blocked")
	}
}

func TestSafeHTTPClientDoesNotFollowRedirects(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "127.0.0.1")

	var redirectTargetRequests atomic.Int32

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectTargetRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	client := newSafeHTTPClient()
	resp, err := client.Get(redirector.URL)
	if err != nil {
		t.Fatalf("request to redirector failed: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusFound)
	}

	if got := redirectTargetRequests.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests, want 0", got)
	}
}
