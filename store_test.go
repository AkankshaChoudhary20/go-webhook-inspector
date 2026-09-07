package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestRequest(id string) *CapturedRequest {
	return &CapturedRequest{
		ID:        id,
		Timestamp: time.Now(),
		Method:    "GET",
		Path:      "/in/test",
		Headers:   map[string][]string{"X-Test": {"1"}},
		Body:      "hello",
	}
}

func TestCreateBinReturnsUniqueID(t *testing.T) {
	s := NewStore()
	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		b, err := s.CreateBin()
		if err != nil {
			t.Fatalf("CreateBin failed: %v", err)
		}
		if b.id == "" {
			t.Fatalf("expected non-empty bin id")
		}
		if seen[b.id] {
			t.Fatalf("duplicate bin id generated: %s", b.id)
		}
		seen[b.id] = true
	}
}

func TestAddRequestAndList(t *testing.T) {
	s := NewStore()
	b, err := s.CreateBin()
	if err != nil {
		t.Fatalf("CreateBin failed: %v", err)
	}

	if err := s.AddRequest(b.id, newTestRequest("req-1")); err != nil {
		t.Fatalf("AddRequest failed: %v", err)
	}
	if err := s.AddRequest(b.id, newTestRequest("req-2")); err != nil {
		t.Fatalf("AddRequest failed: %v", err)
	}

	reqs, err := s.ListRequests(b.id)
	if err != nil {
		t.Fatalf("ListRequests failed: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(reqs))
	}
	// Newest first.
	if reqs[0].ID != "req-2" {
		t.Errorf("expected newest-first order, got first=%s", reqs[0].ID)
	}
	if reqs[1].ID != "req-1" {
		t.Errorf("expected req-1 second, got %s", reqs[1].ID)
	}
}

func TestAddRequestUnknownBin(t *testing.T) {
	s := NewStore()
	err := s.AddRequest("does-not-exist", newTestRequest("req-1"))
	if err != ErrBinNotFound {
		t.Fatalf("expected ErrBinNotFound, got %v", err)
	}
}

func TestGetRequestUnknown(t *testing.T) {
	s := NewStore()
	b, _ := s.CreateBin()
	_, err := s.GetRequest(b.id, "nope")
	if err != ErrRequestNotFound {
		t.Fatalf("expected ErrRequestNotFound, got %v", err)
	}
}

func TestRingBufferEvictionAtCapacity(t *testing.T) {
	s := NewStore()
	b, err := s.CreateBin()
	if err != nil {
		t.Fatalf("CreateBin failed: %v", err)
	}

	total := MaxRequestsPerBin + 25
	for i := 0; i < total; i++ {
		if err := s.AddRequest(b.id, newTestRequest(fmt.Sprintf("req-%d", i))); err != nil {
			t.Fatalf("AddRequest failed at %d: %v", i, err)
		}
	}

	reqs, err := s.ListRequests(b.id)
	if err != nil {
		t.Fatalf("ListRequests failed: %v", err)
	}
	if len(reqs) != MaxRequestsPerBin {
		t.Fatalf("expected exactly %d requests after eviction, got %d", MaxRequestsPerBin, len(reqs))
	}
	// Newest first: the most recently added request should be req-(total-1),
	// and the oldest surviving one should be req-(total-MaxRequestsPerBin).
	wantNewest := fmt.Sprintf("req-%d", total-1)
	if reqs[0].ID != wantNewest {
		t.Errorf("expected newest request %s, got %s", wantNewest, reqs[0].ID)
	}
	wantOldest := fmt.Sprintf("req-%d", total-MaxRequestsPerBin)
	if reqs[len(reqs)-1].ID != wantOldest {
		t.Errorf("expected oldest surviving request %s, got %s", wantOldest, reqs[len(reqs)-1].ID)
	}
}

func TestConcurrentAddAndListIsRaceSafe(t *testing.T) {
	s := NewStore()
	b, err := s.CreateBin()
	if err != nil {
		t.Fatalf("CreateBin failed: %v", err)
	}

	var wg sync.WaitGroup
	writers := 20
	writesPerWriter := 50

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				_ = s.AddRequest(b.id, newTestRequest(fmt.Sprintf("w%d-r%d", w, i)))
			}
		}(w)
	}

	// Concurrent readers via ListRequests.
	stopReaders := make(chan struct{})
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					_, _ = s.ListRequests(b.id)
				}
			}
		}()
	}

	// Concurrent SSE subscribers coming and going.
	for sub := 0; sub < 5; sub++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, unsub, err := s.Subscribe(b.id)
			if err != nil {
				t.Errorf("Subscribe failed: %v", err)
				return
			}
			defer unsub()
			deadline := time.After(50 * time.Millisecond)
			for {
				select {
				case <-deadline:
					return
				case _, ok := <-ch:
					if !ok {
						return
					}
				}
			}
		}()
	}

	// Let writers run, then stop readers once writers are done.
	writersWG := &sync.WaitGroup{}
	_ = writersWG
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(stopReaders)
	}()

	wg.Wait()

	reqs, err := s.ListRequests(b.id)
	if err != nil {
		t.Fatalf("ListRequests failed: %v", err)
	}
	total := writers * writesPerWriter
	want := total
	if want > MaxRequestsPerBin {
		want = MaxRequestsPerBin
	}
	if len(reqs) != want {
		t.Fatalf("expected %d requests stored (capacity-capped), got %d", want, len(reqs))
	}
}

func TestGetBinUnknown(t *testing.T) {
	s := NewStore()
	_, err := s.GetBin("nope")
	if err != ErrBinNotFound {
		t.Fatalf("expected ErrBinNotFound, got %v", err)
	}
}

func TestSubscribePublishesNewRequests(t *testing.T) {
	s := NewStore()
	b, err := s.CreateBin()
	if err != nil {
		t.Fatalf("CreateBin failed: %v", err)
	}

	ch, unsub, err := s.Subscribe(b.id)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer unsub()

	go func() {
		_ = s.AddRequest(b.id, newTestRequest("published-1"))
	}()

	select {
	case req := <-ch:
		if req.ID != "published-1" {
			t.Errorf("expected published-1, got %s", req.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published request")
	}
}
