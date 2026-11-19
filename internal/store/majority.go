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
	Close() error
}

// Result is one instance's outcome for a Put or Get.
type Result struct {
	Addr  string
	Value []byte // Get only; nil when absent or on error
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

func (m *Majority) each(ctx context.Context, op func(context.Context, KV) ([]byte, error)) []Result {
	out := make([]Result, len(m.kvs))
	var wg sync.WaitGroup
	for i, kv := range m.kvs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, m.timeout)
			defer cancel()
			v, err := op(ctx, kv)
			out[i] = Result{Addr: m.addrs[i], Value: v, Err: err}
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

func (r redisKV) Close() error { return r.c.Close() }
