package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

func copyOf(t Topology) store.Result {
	b, _ := json.Marshal(t)
	return store.Result{Value: b}
}

func TestViewNeedsAMajorityForTheSameRecord(t *testing.T) {
	e1 := Topology{Epoch: 1, Primary: "mysql-1", State: StateOK, Claim: "boot"}
	e2claim := Topology{Epoch: 2, State: StatePromoting, Claim: "a"}
	e2done := Topology{Epoch: 2, Primary: "mysql-2", State: StateOK, Claim: "a"}

	tests := []struct {
		name      string
		copies    []store.Result
		wantOK    bool
		wantEpoch uint64
		wantState State
		wantMax   uint64
	}{
		{"all agree", []store.Result{copyOf(e1), copyOf(e1), copyOf(e1)}, true, 1, StateOK, 1},
		{"claim on a minority is not committed", []store.Result{copyOf(e2claim), copyOf(e1), copyOf(e1)}, true, 1, StateOK, 2},
		{"claim on a majority is", []store.Result{copyOf(e2claim), copyOf(e2claim), copyOf(e1)}, true, 2, StatePromoting, 2},
		{"same epoch, different state: neither has a majority", []store.Result{copyOf(e2claim), copyOf(e2done), copyOf(e1)}, false, 0, "", 2},
		{"one instance down", []store.Result{copyOf(e2done), {Err: errors.New("timeout")}, copyOf(e2done)}, true, 2, StateOK, 2},
		{"only one copy reachable", []store.Result{copyOf(e1), {Err: errors.New("down")}, {Err: errors.New("down")}}, false, 0, "", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viewOf(tt.copies, 2)
			if v.OK != tt.wantOK || v.MaxEpoch != tt.wantMax || (v.OK && (v.Committed.Epoch != tt.wantEpoch || v.Committed.State != tt.wantState)) {
				t.Fatalf("got ok=%v epoch=%d state=%s max=%d; want ok=%v epoch=%d state=%s max=%d",
					v.OK, v.Committed.Epoch, v.Committed.State, v.MaxEpoch, tt.wantOK, tt.wantEpoch, tt.wantState, tt.wantMax)
			}
		})
	}
}

// TestScriptsAgainstRedis runs the Lua scripts on real Redis instances. Set
// HAFO_TEST_REDIS to a comma-separated list (e.g. the lab's
// 127.0.0.1:63791,...); it uses throwaway key names.
func TestScriptsAgainstRedis(t *testing.T) {
	addrs := os.Getenv("HAFO_TEST_REDIS")
	if addrs == "" {
		t.Skip("HAFO_TEST_REDIS not set")
	}
	ctx := context.Background()
	m := store.NewRedis(strings.Split(addrs, ","), time.Second)
	defer m.Close()
	key := "hafo:test:topology:" + time.Now().Format("150405.000")
	defer m.Eval(ctx, "return redis.call('DEL', KEYS[1])", []string{key})

	write := func(tp Topology) int {
		b, _ := json.Marshal(tp)
		return store.Replied(m.Eval(ctx, casTopology, []string{key}, string(b)), 1)
	}
	q := m.Size()
	if got := write(Topology{Epoch: 5, Claim: "a", State: StatePromoting}); got != q {
		t.Fatalf("first write accepted by %d/%d", got, q)
	}
	if got := write(Topology{Epoch: 5, Claim: "b", State: StatePromoting}); got != 0 {
		t.Fatalf("a second claimant for the same epoch was accepted by %d", got)
	}
	if got := write(Topology{Epoch: 5, Claim: "a", State: StateOK, Primary: "mysql-2"}); got != q {
		t.Fatalf("the claimant finishing its own epoch was accepted by %d/%d", got, q)
	}
	if got := write(Topology{Epoch: 4, Claim: "c", State: StateOK, Primary: "mysql-1"}); got != 0 {
		t.Fatalf("an older epoch was accepted by %d", got)
	}
	if got := write(Topology{Epoch: 6, Claim: "c", State: StatePromoting}); got != q {
		t.Fatalf("a newer epoch was accepted by %d/%d", got, q)
	}

	leader := "hafo:test:leader:" + time.Now().Format("150405.000")
	defer m.Eval(ctx, "return redis.call('DEL', KEYS[1])", []string{leader})
	acquire := func(id string) int {
		return store.Replied(m.Eval(ctx, acquireLeader, []string{leader}, id, 500), 1)
	}
	if got := acquire("x"); got != q {
		t.Fatalf("free lease granted by %d/%d", got, q)
	}
	if got := acquire("y"); got != 0 {
		t.Fatalf("held lease granted to another id by %d", got)
	}
	if got := acquire("x"); got != q {
		t.Fatalf("holder could not renew: %d/%d", got, q)
	}
	time.Sleep(600 * time.Millisecond)
	if got := acquire("y"); got != q {
		t.Fatalf("expired lease not granted to the next id: %d/%d", got, q)
	}
}
