package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReplayRejectsTrailingJSON(t *testing.T) {
	store := NewStore()
	bin, err := store.CreateBin()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddRequest(bin.id, &CapturedRequest{ID: "request", Method: "GET"}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewServer(store).Routes(mux)
	for _, tail := range []string{"{}", "garbage", strings.Repeat(" ", 1<<16)} {
		req := httptest.NewRequest("POST", "/api/bins/"+bin.id+"/requests/request/replay",
			strings.NewReader(`{"target_url":"http://invalid.example"}`+tail))
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("got %d for invalid trailing input", response.Code)
		}
	}
}
