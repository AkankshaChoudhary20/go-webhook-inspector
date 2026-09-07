package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// Server bundles the in-memory store and HTTP handlers together.
type Server struct {
	store   *Store
	baseURL string // e.g. "" — inbound URLs are returned as relative paths "/in/<id>"
}

// NewServer builds a Server around the given store.
func NewServer(store *Store) *Server {
	return &Server{store: store}
}

// Routes registers all HTTP routes on the given mux.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/bins", s.handleCreateBin)
	mux.HandleFunc("GET /api/bins/{binID}/requests", s.handleListRequests)
	mux.HandleFunc("GET /api/bins/{binID}/stream", s.handleStream)
	mux.HandleFunc("POST /api/bins/{binID}/requests/{reqID}/replay", s.handleReplay)

	// Catch-all inbound capture endpoint: ANY method under /in/<bin_id>[/anything].
	mux.HandleFunc("/in/", s.handleInbound)
}

// --- POST /api/bins ---------------------------------------------------

type createBinResponse struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

func (s *Server) handleCreateBin(w http.ResponseWriter, r *http.Request) {
	b, err := s.store.CreateBin()
	if err != nil {
		http.Error(w, "failed to create bin", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, createBinResponse{
		ID:  b.id,
		URL: "/in/" + b.id,
	})
}

// --- ANY /in/<bin_id> ---------------------------------------------------

func (s *Server) handleInbound(w http.ResponseWriter, r *http.Request) {
	// Path is "/in/<binID>" or "/in/<binID>/rest/of/path".
	rest := strings.TrimPrefix(r.URL.Path, "/in/")
	binID := rest
	if idx := strings.Index(rest, "/"); idx >= 0 {
		binID = rest[:idx]
	}
	if binID == "" {
		http.Error(w, "missing bin id", http.StatusBadRequest)
		return
	}

	if _, err := s.store.GetBin(binID); err != nil {
		http.Error(w, "unknown bin", http.StatusNotFound)
		return
	}

	limited := io.LimitReader(r.Body, MaxBodyBytes+1)
	bodyBytes, err := io.ReadAll(limited)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	truncated := false
	if len(bodyBytes) > MaxBodyBytes {
		bodyBytes = bodyBytes[:MaxBodyBytes]
		truncated = true
	}

	reqID, err := randomID()
	if err != nil {
		http.Error(w, "failed to generate id", http.StatusInternalServerError)
		return
	}

	captured := &CapturedRequest{
		ID:          reqID,
		Timestamp:   time.Now(),
		Method:      r.Method,
		Path:        r.URL.Path,
		Headers:     map[string][]string(r.Header),
		QueryParams: map[string][]string(r.URL.Query()),
		Body:        string(bodyBytes),
		BodyTrunc:   truncated,
		RemoteAddr:  r.RemoteAddr,
	}

	if err := s.store.AddRequest(binID, captured); err != nil {
		http.Error(w, "unknown bin", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// --- GET /api/bins/{binID}/requests ---------------------------------------------------

func (s *Server) handleListRequests(w http.ResponseWriter, r *http.Request) {
	binID := r.PathValue("binID")
	reqs, err := s.store.ListRequests(binID)
	if err != nil {
		http.Error(w, "unknown bin", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, reqs)
}

// --- GET /api/bins/{binID}/stream (SSE) ---------------------------------------------------

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	binID := r.PathValue("binID")

	ch, unsubscribe, err := s.store.Subscribe(binID)
	if err != nil {
		http.Error(w, "unknown bin", http.StatusNotFound)
		return
	}
	defer unsubscribe()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Initial comment to open the stream promptly for the client.
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	ctx := r.Context()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepalive.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		case req, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(req)
			if err != nil {
				log.Printf("sse: marshal error: %v", err)
				continue
			}
			fmt.Fprintf(w, "event: request\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// --- POST /api/bins/{binID}/requests/{reqID}/replay ---------------------------------------------------

type replayRequest struct {
	TargetURL string `json:"target_url"`
}

type replayResponse struct {
	Status     int               `json:"status"`
	StatusText string            `json:"status_text"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	Error      string            `json:"error,omitempty"`
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	binID := r.PathValue("binID")
	reqID := r.PathValue("reqID")

	captured, err := s.store.GetRequest(binID, reqID)
	if err != nil {
		http.Error(w, "unknown bin or request", http.StatusNotFound)
		return
	}

	var body replayRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body); err != nil {
		http.Error(w, "invalid JSON body: expected {\"target_url\": \"...\"}", http.StatusBadRequest)
		return
	}
	if body.TargetURL == "" {
		http.Error(w, "target_url is required", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(body.TargetURL, "http://") && !strings.HasPrefix(body.TargetURL, "https://") {
		http.Error(w, "target_url must start with http:// or https://", http.StatusBadRequest)
		return
	}

	outReq, err := http.NewRequestWithContext(r.Context(), captured.Method, body.TargetURL, bytes.NewReader([]byte(captured.Body)))
	if err != nil {
		http.Error(w, "failed to build replay request: "+err.Error(), http.StatusBadRequest)
		return
	}
	for k, vals := range captured.Headers {
		lower := strings.ToLower(k)
		// Skip hop-by-hop / connection-specific headers that must not be
		// forwarded verbatim to a new destination.
		if lower == "host" || lower == "content-length" || lower == "connection" {
			continue
		}
		for _, v := range vals {
			outReq.Header.Add(k, v)
		}
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(outReq)
	if err != nil {
		writeJSON(w, http.StatusOK, replayResponse{Error: err.Error()})
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, MaxBodyBytes))
	if err != nil {
		writeJSON(w, http.StatusOK, replayResponse{Error: "failed to read target response: " + err.Error()})
		return
	}

	headers := make(map[string]string, len(resp.Header))
	for k, vals := range resp.Header {
		headers[k] = strings.Join(vals, ", ")
	}

	writeJSON(w, http.StatusOK, replayResponse{
		Status:     resp.StatusCode,
		StatusText: resp.Status,
		Headers:    headers,
		Body:       string(respBody),
	})
}

// --- helpers ---------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON: encode error: %v", err)
	}
}
