package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReplayRejectsTrailingJSONBeforeSending(t *testing.T) {
	store := NewStore()
	bin, err := store.CreateBin()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddRequest(bin.id, &CapturedRequest{ID: "req", Method: "POST"}); err != nil {
		t.Fatal(err)
	}
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	http.DefaultTransport = replayTransport(func(*http.Request) (*http.Response, error) { t.Fatal("sent invalid replay"); return nil, nil })
	mux := http.NewServeMux()
	NewServer(store).Routes(mux)
	for _, suffix := range []string{` {}`, ` null`, ` garbage`, strings.Repeat(" ", 1<<16)} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest("POST", "/api/bins/"+bin.id+"/requests/req/replay", strings.NewReader(`{"target_url":"http://example.test"}`+suffix)))
		if response.Code != 400 {
			t.Errorf("got %d", response.Code)
		}
	}
}
