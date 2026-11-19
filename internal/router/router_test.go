package router

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
)

type fakeSource struct {
	mu   sync.Mutex
	topo cluster.Topology
	pool []string
}

func (f *fakeSource) set(t cluster.Topology) { f.mu.Lock(); f.topo = t; f.mu.Unlock() }

func (f *fakeSource) Topology(context.Context) cluster.View {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cluster.View{Committed: f.topo, OK: true, Holders: 3}
}

func (f *fakeSource) ReadPool(context.Context) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pool
}

// backend greets each connection with its name, like a MySQL server sends
// its handshake first, then holds the connection open.
func backend(t *testing.T, name string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, _ = c.Write([]byte(name + "\n"))
				_, _ = io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

func greet(t *testing.T, addr string) (string, net.Conn) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		c.Close()
		return "", nil // refused: the router closed the connection
	}
	_ = c.SetReadDeadline(time.Time{})
	return line[:len(line)-1], c
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func TestRouterFollowsTheTopology(t *testing.T) {
	addrs := map[string]string{"mysql-1": backend(t, "mysql-1"), "mysql-2": backend(t, "mysql-2"), "mysql-3": backend(t, "mysql-3")}
	src := &fakeSource{topo: cluster.Topology{Epoch: 1, Primary: "mysql-1", State: cluster.StateOK}, pool: []string{"mysql-2", "mysql-3"}}
	cfg := Config{WriteListen: freeAddr(t), ReadListen: freeAddr(t), Poll: 10 * time.Millisecond, DialTimeout: time.Second}
	r := New(cfg, slog.New(slog.DiscardHandler), src, func(n string) string { return addrs[n] }, prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}

	got, oldConn := greet(t, cfg.WriteListen)
	if got != "mysql-1" {
		t.Fatalf("write port reached %q, want mysql-1", got)
	}
	if a, _ := greet(t, cfg.ReadListen); a != "mysql-2" && a != "mysql-3" {
		t.Fatalf("read port reached %q, want a replica", a)
	}

	// A claim: no primary. Existing write connections close; new ones are refused.
	src.set(cluster.Topology{Epoch: 2, State: cluster.StatePromoting})
	time.Sleep(50 * time.Millisecond)
	_ = oldConn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := oldConn.Read(make([]byte, 1)); err == nil {
		t.Fatal("write connection to the old primary survived the epoch change")
	}
	if got, _ := greet(t, cfg.WriteListen); got != "" {
		t.Fatalf("write accepted to %q while promoting", got)
	}

	// Published: writes go to the new primary.
	src.set(cluster.Topology{Epoch: 2, Primary: "mysql-2", State: cluster.StateOK})
	time.Sleep(50 * time.Millisecond)
	if got, _ := greet(t, cfg.WriteListen); got != "mysql-2" {
		t.Fatalf("write port reached %q after failover, want mysql-2", got)
	}

	// An older epoch read later (e.g. a lagging copy) must not move it back.
	src.set(cluster.Topology{Epoch: 1, Primary: "mysql-1", State: cluster.StateOK})
	time.Sleep(50 * time.Millisecond)
	if got, _ := greet(t, cfg.WriteListen); got != "mysql-2" {
		t.Fatalf("an older epoch moved writes back to %q", got)
	}
}
