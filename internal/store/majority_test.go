package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeKV struct {
	mu   sync.Mutex
	data map[string][]byte
	err  error         // returned by every call when set
	hang time.Duration // delay before answering, to exercise the timeout
}

func newFake() *fakeKV { return &fakeKV{data: map[string][]byte{}} }

func (f *fakeKV) wait(ctx context.Context) error {
	if f.hang > 0 {
		select {
		case <-time.After(f.hang):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func (f *fakeKV) Set(ctx context.Context, key string, val []byte, _ time.Duration) error {
	if err := f.wait(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = val
	return nil
}

func (f *fakeKV) Get(ctx context.Context, key string) ([]byte, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key], nil
}

func (f *fakeKV) Close() error { return nil }

func TestQuorum(t *testing.T) {
	for n, want := range map[int]int{1: 1, 2: 2, 3: 2, 5: 3} {
		m := New(make([]string, n), make([]KV, n), time.Second)
		if got := m.Quorum(); got != want {
			t.Errorf("quorum of %d = %d, want %d", n, got, want)
		}
	}
}

func TestPutAndGetReportEachInstance(t *testing.T) {
	ok1, ok2, broken := newFake(), newFake(), newFake()
	broken.err = errors.New("connection refused")
	m := New([]string{"a", "b", "c"}, []KV{ok1, ok2, broken}, time.Second)

	rs := m.Put(context.Background(), "k", []byte("v"), time.Second)
	if got := Succeeded(rs); got != 2 {
		t.Fatalf("Succeeded = %d, want 2", got)
	}
	if rs[2].Addr != "c" || rs[2].Err == nil {
		t.Fatalf("result for the broken instance: %+v", rs[2])
	}

	got := m.Get(context.Background(), "k")
	if string(got[0].Value) != "v" || string(got[1].Value) != "v" || got[2].Err == nil {
		t.Fatalf("Get results: %+v", got)
	}
	if missing := m.Get(context.Background(), "nope"); missing[0].Value != nil || missing[0].Err != nil {
		t.Fatalf("absent key should be nil value, nil error: %+v", missing[0])
	}
}

func TestSlowInstanceDoesNotStallTheOthers(t *testing.T) {
	slow := newFake()
	slow.hang = time.Second
	m := New([]string{"a", "b", "c"}, []KV{newFake(), newFake(), slow}, 50*time.Millisecond)

	start := time.Now()
	rs := m.Put(context.Background(), "k", []byte("v"), time.Second)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Put took %v; the per-instance timeout did not apply", elapsed)
	}
	if Succeeded(rs) != 2 || !errors.Is(rs[2].Err, context.DeadlineExceeded) {
		t.Fatalf("want 2 successes and a deadline error, got %+v", rs)
	}
}
