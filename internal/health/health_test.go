package health

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

func lagOf(s float64) *float64 { return &s }

func replica() Observation {
	return Observation{Reachable: true, ReadOnly: true, SuperReadOnly: true,
		SourceHost: "mysql-1", IOState: "ON", SQLState: "ON", LagSeconds: lagOf(0.1)}
}

func primary() Observation {
	return Observation{Reachable: true, SemiSyncSourceEnabled: true, SemiSyncSourceActive: true, SemiSyncClients: 2}
}

func codes(rs []Reason) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Code
	}
	return out
}

func TestEvaluate(t *testing.T) {
	maxLag := time.Second
	tests := []struct {
		name string
		obs  func() Observation
		want []string
	}{
		{"healthy replica", replica, nil},
		{"healthy primary", primary, nil},
		{"lag at threshold is still healthy", func() Observation { o := replica(); o.LagSeconds = lagOf(1.0); return o }, nil},
		{"lag over threshold", func() Observation { o := replica(); o.LagSeconds = lagOf(1.2); return o }, []string{CodeLagHigh}},
		{"no heartbeat row", func() Observation { o := replica(); o.LagSeconds = nil; return o }, []string{CodeLagUnknown}},
		{"applier stopped", func() Observation { o := replica(); o.SQLState = "OFF"; return o }, []string{CodeSQLThread}},
		{"IO thread reconnecting and lagging", func() Observation {
			o := replica()
			o.IOState, o.LagSeconds = "CONNECTING", lagOf(3)
			return o
		}, []string{CodeIOThread, CodeLagHigh}},
		{"read-only without a source", func() Observation { return Observation{Reachable: true, ReadOnly: true} }, []string{CodeFenced}},
		{"primary without semi-sync", func() Observation { o := primary(); o.SemiSyncSourceEnabled = false; return o }, []string{CodeSemiSyncOff}},
		{"primary with no semi-sync replicas", func() Observation { o := primary(); o.SemiSyncClients = 0; return o }, []string{CodeNoSemiSyncPeers}},
		{"primary with blocked commits", func() Observation { o := primary(); o.BlockedWrites = 1; return o }, []string{CodeWritesBlocked}},
		{"writable while replicating", func() Observation { o := primary(); o.SourceHost = "mysql-2"; return o }, []string{CodeWritableReplica}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := codes(Evaluate(tt.obs(), maxLag)); !slices.Equal(got, tt.want) && !(len(got) == 0 && len(tt.want) == 0) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTrackerDebouncesUnreachable(t *testing.T) {
	now := time.Unix(0, 0)
	tick := func() time.Time { now = now.Add(250 * time.Millisecond); return now }
	tr := NewTracker(Thresholds{FailAfter: 3, RecoverAfter: 2, MaxLag: time.Second}, now)

	if v := tr.Observe(replica(), tick()); v.Healthy || v.Reasons[0].Code != CodeRecovering {
		t.Fatalf("first clean probe after start: %+v, want recovering", v)
	}
	v := tr.Observe(replica(), tick())
	if !v.Healthy {
		t.Fatalf("second clean probe: %+v, want healthy", v)
	}
	healthySince := v.Since

	down := Observation{ProbeError: "i/o timeout"}
	for i := 1; i <= 2; i++ {
		if v := tr.Observe(down, tick()); !v.Healthy {
			t.Fatalf("failure %d of 3 already unhealthy: %+v", i, v)
		}
	}
	v = tr.Observe(down, tick())
	if v.Healthy || v.Reasons[0].Code != CodeUnreachable || v.Reasons[0].Detail != "i/o timeout" {
		t.Fatalf("third failure: %+v, want unreachable", v)
	}
	if !v.Since.After(healthySince) {
		t.Fatalf("Since did not move on the flip to unhealthy")
	}

	// A success between failures resets the count.
	tr.Observe(replica(), tick())
	tr.Observe(replica(), tick())
	tr.Observe(down, tick())
	tr.Observe(down, tick())
	tr.Observe(replica(), tick())
	tr.Observe(down, tick())
	if v := tr.Observe(down, tick()); !v.Healthy {
		t.Fatalf("non-consecutive failures flipped the verdict: %+v", v)
	}
}

func TestTrackerReportedProblemsFlipAtOnce(t *testing.T) {
	now := time.Unix(0, 0)
	tr := NewTracker(Thresholds{FailAfter: 3, RecoverAfter: 2, MaxLag: time.Second}, now)
	tr.Observe(replica(), now)
	tr.Observe(replica(), now)

	stopped := replica()
	stopped.SQLState = "OFF"
	if v := tr.Observe(stopped, now); v.Healthy {
		t.Fatal("stopped applier should be unhealthy on the first probe")
	}
	if v := tr.Observe(replica(), now); v.Healthy {
		t.Fatal("one clean probe should not be enough to recover")
	}
	if v := tr.Observe(replica(), now); !v.Healthy {
		t.Fatal("two clean probes should recover")
	}
}

func TestLatestPicksFreshestAndCountsHolders(t *testing.T) {
	enc := func(r Record) []byte { b, _ := json.Marshal(r); return b }
	t0 := time.Unix(100, 0)
	old := Record{Node: "mysql-3", Boot: "a", Seq: 7, ObservedAt: t0}
	fresh := Record{Node: "mysql-3", Boot: "a", Seq: 8, ObservedAt: t0.Add(time.Millisecond)}
	rebooted := Record{Node: "mysql-3", Boot: "b", Seq: 1, ObservedAt: t0.Add(time.Second)}

	rec, holders, ok := Latest([]store.Result{
		{Addr: "r1", Value: enc(old)},
		{Addr: "r2", Value: enc(fresh)},
		{Addr: "r3", Err: errors.New("timeout")},
	})
	if !ok || holders != 2 || rec.Seq != 8 {
		t.Fatalf("got seq %d holders %d ok %v, want seq 8 from 2 holders", rec.Seq, holders, ok)
	}

	rec, _, _ = Latest([]store.Result{{Value: enc(fresh)}, {Value: enc(rebooted)}})
	if rec.Boot != "b" {
		t.Fatalf("a newer boot should win by observation time, got boot %q", rec.Boot)
	}

	if _, holders, ok := Latest([]store.Result{{}, {Value: []byte("not json")}}); ok || holders != 0 {
		t.Fatalf("absent and corrupt copies should not count: holders %d ok %v", holders, ok)
	}
}
