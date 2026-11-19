package coordinator

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDetectorNeedsBothObserversThenTheLease(t *testing.T) {
	d := detector{failAfter: 2, margin: 300 * time.Millisecond}
	now := time.Unix(0, 0)
	step := func(ev evidence) bool { now = now.Add(250 * time.Millisecond); return d.observe(ev, now) }
	down := evidence{coordDown: true, agentDown: true}
	gone := evidence{coordDown: true, agentDown: true, leaseAbsent: true}

	if step(evidence{coordDown: true}) || step(evidence{agentDown: true, leaseAbsent: true}) {
		t.Fatal("one observer alone must never trigger a failover")
	}
	if d.ticks != 0 {
		t.Fatal("a tick where the observers disagree must reset detection")
	}
	if step(down) || step(down) || step(down) {
		t.Fatal("both observers agreeing is not enough while the primary still holds its lease")
	}
	// The margin is checked on ticks: 300ms at a 250ms tick means two ticks.
	if step(gone) || step(gone) {
		t.Fatal("must wait the margin after the lease disappears")
	}
	if !step(gone) || !d.since.Equal(time.Unix(0, 0).Add(3*250*time.Millisecond)) {
		t.Fatalf("expected failover after the margin, suspected since the first agreeing tick; since=%v", d.since)
	}
}

func TestDetectorLeaseRenewalResetsTheMargin(t *testing.T) {
	d := detector{failAfter: 1, margin: 300 * time.Millisecond}
	now := time.Unix(0, 0)
	gone := evidence{coordDown: true, agentDown: true, leaseAbsent: true}
	held := evidence{coordDown: true, agentDown: true}
	d.observe(gone, now)
	d.observe(held, now.Add(250*time.Millisecond)) // primary renewed: a stall, not a death
	if d.observe(gone, now.Add(500*time.Millisecond)) {
		t.Fatal("the margin must restart after the lease reappears")
	}
	if !d.observe(gone, now.Add(800*time.Millisecond)) {
		t.Fatal("expected failover once the lease stayed gone for the margin")
	}
}

// gtidSets models GTID sets as sets of transaction labels, e.g. "a b c".
func subsetOf(a, b string) (bool, error) {
	have := map[string]bool{}
	for _, x := range strings.Fields(b) {
		have[x] = true
	}
	for _, x := range strings.Fields(a) {
		if !have[x] {
			return false, nil
		}
	}
	return true, nil
}

func TestChoose(t *testing.T) {
	tests := []struct {
		name    string
		cands   []candidate
		want    string
		wantErr string
	}{
		{"most advanced wins", []candidate{{node: "mysql-2", gtid: "1 2"}, {node: "mysql-3", gtid: "1 2 3"}}, "mysql-3", ""},
		{"equal sets go to the first name", []candidate{{node: "mysql-3", gtid: "1 2"}, {node: "mysql-2", gtid: "1 2"}}, "mysql-2", ""},
		{"unprepared candidates are skipped", []candidate{{node: "mysql-2", gtid: "1 2 3"}, {node: "mysql-3", err: errors.New("timeout")}}, "mysql-2", ""},
		{"diverged replicas stop the failover", []candidate{{node: "mysql-2", gtid: "1 2 x"}, {node: "mysql-3", gtid: "1 2 y"}}, "", "diverged"},
		{"nobody prepared", []candidate{{node: "mysql-2", err: errors.New("down")}}, "", "no reachable replica"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := choose(tt.cands, subsetOf)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to mention %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
	if _, err := choose(nil, subsetOf); !errors.Is(err, errNoCandidate) {
		t.Fatalf("empty candidate list should be errNoCandidate, got %v", err)
	}
}
