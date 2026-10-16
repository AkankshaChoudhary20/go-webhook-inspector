package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type replayTransport func(*http.Request) (*http.Response, error)

func (f replayTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestReplayStripsConnectionHeadersAndPreservesPayload(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	http.DefaultTransport = replayTransport(func(r *http.Request) (*http.Response, error) {
		for _, header := range []string{"Connection", "Keep-Alive", "Proxy-Authorization", "Te", "Trailer", "Upgrade", "X-Private", "X-Other"} {
			if r.Header.Get(header) != "" {
				t.Errorf("forwarded %s", header)
			}
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "hello" || r.Method != "POST" || r.Header.Get("X-Custom") != "kept" {
			t.Errorf("lost original payload: method=%s body=%s headers=%v", r.Method, body, r.Header)
		}
		return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("created"))}, nil
	})
	store := NewStore()
	bin, err := store.CreateBin()
	if err != nil {
		t.Fatal(err)
	}
	err = store.AddRequest(bin.id, &CapturedRequest{ID: "req", Method: "POST", Body: "hello", Headers: map[string][]string{
		"cOnNeCtIoN": {"keep-alive, X-Private", " X-Other "},
		"Keep-Alive": {"timeout=5"}, "Proxy-Authorization": {"secret"},
		"Te": {"trailers"}, "Trailer": {"X-Trailer"}, "Upgrade": {"websocket"},
		"X-Private": {"private"}, "X-Other": {"other"}, "X-Custom": {"kept"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewServer(store).Routes(mux)
	body, _ := json.Marshal(replayRequest{TargetURL: "http://example.test/replay"})
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest("POST", "/api/bins/"+bin.id+"/requests/req/replay", bytes.NewReader(body)))
	var result replayResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || result.Status != http.StatusCreated || result.Error != "" {
		t.Fatalf("replay failed: %d %+v", recorder.Code, result)
	}
}

func TestReplayRejectsTruncatedCapturedBodies(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	called := false
	http.DefaultTransport = replayTransport(func(r *http.Request) (*http.Response, error) { called = true; return nil, fmt.Errorf("must not send") })
	store := NewStore()
	bin, err := store.CreateBin()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddRequest(bin.id, &CapturedRequest{ID: "truncated", Method: "POST", Body: "partial", BodyTrunc: true}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewServer(store).Routes(mux)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/bins/"+bin.id+"/requests/truncated/replay", strings.NewReader(`{"target_url":"https://example.test"}`))
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("got status %d body %s", response.Code, response.Body.String())
	}
	if called {
		t.Fatal("sent a partial captured request")
	}
}
