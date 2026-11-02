package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type redirectTransport func(*http.Request) (*http.Response, error)

func (f redirectTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestReplayReturnsRedirectWithoutChangingRequest(t *testing.T) {
	original := http.DefaultTransport
	defer func() { http.DefaultTransport = original }()
	calls := 0
	http.DefaultTransport = redirectTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		status, headers := 302, make(http.Header)
		headers.Set("Location", "http://target.test/next")
		if calls > 1 {
			status = 204
			headers.Del("Location")
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: headers, Body: io.NopCloser(strings.NewReader("redirect")), Request: r}, nil
	})
	store := NewStore()
	bin, _ := store.CreateBin()
	store.AddRequest(bin.id, &CapturedRequest{ID: "request", Method: "POST", Body: "original"})
	mux := http.NewServeMux()
	NewServer(store).Routes(mux)
	body, _ := json.Marshal(map[string]string{"target_url": "http://target.test"})
	request := httptest.NewRequest("POST", "/api/bins/"+bin.id+"/requests/request/replay", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	var response replayResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Status != 302 || calls != 1 {
		t.Fatalf("redirect was followed: status=%d calls=%d", response.Status, calls)
	}
}
