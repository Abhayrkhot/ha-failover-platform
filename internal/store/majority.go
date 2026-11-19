// Package store reads and writes small values on several independent Redis
// instances at once. It reports each instance's outcome and the quorum size;
// callers decide what a majority means for their value.
package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// KV is the subset of a Redis client the majority needs.
type KV interface {
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	// Get returns nil, nil when the key is absent.
	Get(ctx context.Context, key string) ([]byte, error)
	// Eval runs a Lua script that returns an integer. Scripts are how
	// compare-and-set and lease operations stay atomic on each instance.
	Eval(ctx context.Context, script string, keys []string, args ...any) (int64, error)
	Close() error
}

// Result is one instance's outcome for a Put or Get.
type Result struct {
	Addr  string
	Value []byte // Get only; nil when absent or on error
	N     int64  // Eval only: the script's integer reply
	Err   error
}

// Majority fans operations out to every instance in parallel, each bounded by
// its own timeout so one hung instance cannot stall the others.
type Majority struct {
	addrs   []string
	kvs     []KV
	timeout time.Duration
}

// NewRedis connects to each address with fail-fast settings: no retries, and
// dial/read/write bounded by timeout.
func NewRedis(addrs []string, timeout time.Duration) *Majority {
	kvs := make([]KV, len(addrs))
	for i, a := range addrs {
		kvs[i] = redisKV{redis.NewClient(&redis.Options{
			Addr:                  a,
			DialTimeout:           timeout,
			ReadTimeout:           timeout,
			WriteTimeout:          timeout,
			ContextTimeoutEnabled: true,
			MaxRetries:            -1,
			DialerRetries:         1, // default 5: a dead instance would be re-dialled within one call
			PoolSize:              4,
		})}
	}
	return New(addrs, kvs, timeout)
}

// New builds a Majority over existing clients (used by tests).
func New(addrs []string, kvs []KV, timeout time.Duration) *Majority {
	return &Majority{addrs: addrs, kvs: kvs, timeout: timeout}
}

// Size is the number of instances.
func (m *Majority) Size() int { return len(m.kvs) }

// Quorum is the smallest majority of instances.
func (m *Majority) Quorum() int { return len(m.kvs)/2 + 1 }

// Put sets key on every instance with the given TTL.
func (m *Majority) Put(ctx context.Context, key string, val []byte, ttl time.Duration) []Result {
	return m.each(ctx, func(ctx context.Context, kv KV) ([]byte, error) {
		return nil, kv.Set(ctx, key, val, ttl)
	})
}

// Get reads key from every instance.
func (m *Majority) Get(ctx context.Context, key string) []Result {
	return m.each(ctx, func(ctx context.Context, kv KV) ([]byte, error) {
		return kv.Get(ctx, key)
	})
}

// Eval runs script on every instance.
func (m *Majority) Eval(ctx context.Context, script string, keys []string, args ...any) []Result {
	return m.run(ctx, func(ctx context.Context, kv KV, r *Result) {
		r.N, r.Err = kv.Eval(ctx, script, keys, args...)
	})
}

func (m *Majority) each(ctx context.Context, op func(context.Context, KV) ([]byte, error)) []Result {
	return m.run(ctx, func(ctx context.Context, kv KV, r *Result) {
		r.Value, r.Err = op(ctx, kv)
	})
}

func (m *Majority) run(ctx context.Context, op func(context.Context, KV, *Result)) []Result {
	out := make([]Result, len(m.kvs))
	var wg sync.WaitGroup
	for i, kv := range m.kvs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			out[i].Addr = m.addrs[i]
			op(ctx, kv, &out[i])
		}()
	}
	wg.Wait()
	return out
}

// Close closes every client.
func (m *Majority) Close() error {
	var errs []error
	for _, kv := range m.kvs {
		errs = append(errs, kv.Close())
	}
	return errors.Join(errs...)
}

// Replied counts Eval results whose script returned want.
func Replied(rs []Result, want int64) int {
	n := 0
	for _, r := range rs {
		if r.Err == nil && r.N == want {
			n++
		}
	}
	return n
}

// Absent counts instances that answered and do not hold the key.
func Absent(rs []Result) int {
	n := 0
	for _, r := range rs {
		if r.Err == nil && r.Value == nil {
			n++
		}
	}
	return n
}

// Succeeded counts results without an error.
func Succeeded(rs []Result) int {
	n := 0
	for _, r := range rs {
		if r.Err == nil {
			n++
		}
	}
	return n
}

type redisKV struct{ c *redis.Client }

func (r redisKV) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	return r.c.Set(ctx, key, val, ttl).Err()
}

func (r redisKV) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := r.c.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}

func (r redisKV) Eval(ctx context.Context, script string, keys []string, args ...any) (int64, error) {
	return r.c.Eval(ctx, script, keys, args...).Int64()
}

func (r redisKV) Close() error { return r.c.Close() }
