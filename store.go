package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// MaxRequestsPerBin is the number of most-recent captured requests kept per bin.
const MaxRequestsPerBin = 100

// MaxBodyBytes caps how much of an inbound request body we store.
const MaxBodyBytes = 1 << 20 // 1MB

// ErrBinNotFound is returned when a bin id does not exist in the store.
var ErrBinNotFound = errors.New("bin not found")

// ErrRequestNotFound is returned when a request id does not exist within a bin.
var ErrRequestNotFound = errors.New("request not found")

// CapturedRequest represents one inbound HTTP request captured for a bin.
type CapturedRequest struct {
	ID          string              `json:"id"`
	Timestamp   time.Time           `json:"timestamp"`
	Method      string              `json:"method"`
	Path        string              `json:"path"`
	Headers     map[string][]string `json:"headers"`
	QueryParams map[string][]string `json:"query_params"`
	Body        string              `json:"body"`
	BodyTrunc   bool                `json:"body_truncated"`
	RemoteAddr  string              `json:"remote_addr"`
}

// bin holds the captured requests for a single inbound endpoint plus the set
// of live SSE subscribers waiting for new requests.
type bin struct {
	id        string
	createdAt time.Time

	mu       sync.Mutex
	requests []*CapturedRequest // newest first, capped at MaxRequestsPerBin

	subMu sync.Mutex
	subs  map[chan *CapturedRequest]struct{}
}

func newBin(id string) *bin {
	return &bin{
		id:        id,
		createdAt: time.Now(),
		requests:  make([]*CapturedRequest, 0, MaxRequestsPerBin),
		subs:      make(map[chan *CapturedRequest]struct{}),
	}
}

// add prepends a new captured request, evicting the oldest once the bin is
// at capacity (ring-buffer semantics over a newest-first slice).
func (b *bin) add(req *CapturedRequest) {
	b.mu.Lock()
	b.requests = append([]*CapturedRequest{req}, b.requests...)
	if len(b.requests) > MaxRequestsPerBin {
		b.requests = b.requests[:MaxRequestsPerBin]
	}
	b.mu.Unlock()

	b.publish(req)
}

// list returns a copy of the currently captured requests, newest first.
func (b *bin) list() []*CapturedRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*CapturedRequest, len(b.requests))
	copy(out, b.requests)
	return out
}

// get returns a single captured request by id.
func (b *bin) get(reqID string) (*CapturedRequest, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.requests {
		if r.ID == reqID {
			return r, nil
		}
	}
	return nil, ErrRequestNotFound
}

// subscribe registers a new channel that receives every request captured
// from this point forward. Call the returned function to unsubscribe.
func (b *bin) subscribe() (chan *CapturedRequest, func()) {
	ch := make(chan *CapturedRequest, 16)
	b.subMu.Lock()
	b.subs[ch] = struct{}{}
	b.subMu.Unlock()

	unsubscribe := func() {
		b.subMu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.subMu.Unlock()
	}
	return ch, unsubscribe
}

// publish fans a newly captured request out to all live subscribers,
// without blocking on a slow/dead reader.
func (b *bin) publish(req *CapturedRequest) {
	b.subMu.Lock()
	defer b.subMu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- req:
		default:
			// Subscriber is too slow to keep up; drop this event for it
			// rather than blocking the inbound capture path.
		}
	}
}

// Store is the concurrency-safe, in-memory collection of all bins.
type Store struct {
	mu   sync.RWMutex
	bins map[string]*bin
}

// NewStore creates an empty Store.
func NewStore() *Store {
	return &Store{bins: make(map[string]*bin)}
}

// CreateBin allocates a new bin with a random unique id and registers it.
func (s *Store) CreateBin() (*bin, error) {
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	b := newBin(id)

	s.mu.Lock()
	defer s.mu.Unlock()
	// Vanishingly unlikely, but guard against id collisions anyway.
	for {
		if _, exists := s.bins[id]; !exists {
			break
		}
		id, err = randomID()
		if err != nil {
			return nil, err
		}
		b.id = id
	}
	s.bins[id] = b
	return b, nil
}

// GetBin looks up a bin by id.
func (s *Store) GetBin(id string) (*bin, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bins[id]
	if !ok {
		return nil, ErrBinNotFound
	}
	return b, nil
}

// AddRequest captures a new request into the named bin.
func (s *Store) AddRequest(binID string, req *CapturedRequest) error {
	b, err := s.GetBin(binID)
	if err != nil {
		return err
	}
	b.add(req)
	return nil
}

// ListRequests returns the captured requests for a bin, newest first.
func (s *Store) ListRequests(binID string) ([]*CapturedRequest, error) {
	b, err := s.GetBin(binID)
	if err != nil {
		return nil, err
	}
	return b.list(), nil
}

// GetRequest fetches a single captured request from a bin.
func (s *Store) GetRequest(binID, reqID string) (*CapturedRequest, error) {
	b, err := s.GetBin(binID)
	if err != nil {
		return nil, err
	}
	return b.get(reqID)
}

// Subscribe registers an SSE-style subscriber for a bin's new requests.
func (s *Store) Subscribe(binID string) (chan *CapturedRequest, func(), error) {
	b, err := s.GetBin(binID)
	if err != nil {
		return nil, nil, err
	}
	ch, unsub := b.subscribe()
	return ch, unsub, nil
}

func randomID() (string, error) {
	buf := make([]byte, 8) // 16 hex chars
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
