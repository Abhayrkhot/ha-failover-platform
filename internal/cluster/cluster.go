// Package cluster is the cluster-wide state kept on the Redis majority: the
// topology (which node is primary, at which epoch), the primary's lease, the
// coordinator leader's lease, and the record of the last failover.
//
// Every value lives on all instances and counts only when a majority agree.
// Writes that must not race (topology, leases) are Lua scripts, so each
// instance applies them atomically.
package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

const (
	TopologyKey     = "hafo:topology"
	PrimaryLeaseKey = "hafo:lease:primary"
	LeaderKey       = "hafo:leader"
	FailoverKey     = "hafo:failover:last"
)

// State of a topology epoch.
type State string

const (
	StateOK        State = "ok"        // Primary is serving writes
	StatePromoting State = "promoting" // epoch claimed, no primary yet: writes refused
	StateFailed    State = "failed"    // promotion aborted; needs a human (see Reason)
)

// Topology says which node is primary. Epoch only ever increases and is the
// fencing token: anything acting on an older epoch is stale.
type Topology struct {
	Epoch     uint64    `json:"epoch"`
	Primary   string    `json:"primary"`
	State     State     `json:"state"`
	Previous  string    `json:"previous,omitempty"`
	Claim     string    `json:"claim"` // identifies who wrote this epoch
	By        string    `json:"by"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Serving reports whether t names a primary that should take writes.
func (t Topology) Serving() bool { return t.State == StateOK && t.Primary != "" }

// View is what a reader can trust from the copies on each instance.
type View struct {
	Committed Topology // highest epoch held identically by a majority
	OK        bool     // a committed topology exists
	Holders   int      // instances holding Committed
	MaxEpoch  uint64   // highest epoch on any instance, committed or not
	Reachable int      // instances that answered
}

// ReadTopology reads every copy and returns the committed view.
func ReadTopology(ctx context.Context, m *store.Majority) View {
	return viewOf(m.Get(ctx, TopologyKey), m.Quorum())
}

func viewOf(results []store.Result, quorum int) View {
	type id struct {
		epoch   uint64
		claim   string
		state   State
		primary string
	}
	var v View
	counts := map[id]int{}
	recs := map[id]Topology{}
	for _, r := range results {
		if r.Err != nil {
			continue
		}
		v.Reachable++
		if r.Value == nil {
			continue
		}
		var t Topology
		if json.Unmarshal(r.Value, &t) != nil {
			continue
		}
		k := id{t.Epoch, t.Claim, t.State, t.Primary}
		counts[k]++
		recs[k] = t
		v.MaxEpoch = max(v.MaxEpoch, t.Epoch)
	}
	for k, n := range counts {
		if n >= quorum && (!v.OK || k.epoch > v.Committed.Epoch) {
			v.Committed, v.Holders, v.OK = recs[k], n, true
		}
	}
	return v
}

// casTopology stores the new topology unless the instance already holds a
// higher epoch, or the same epoch written by a different claimant. Two
// coordinators racing for epoch N can therefore never both win a majority.
const casTopology = `
local cur = redis.call('GET', KEYS[1])
local new = cjson.decode(ARGV[1])
if cur then
  local c = cjson.decode(cur)
  if c.epoch > new.epoch then return 0 end
  if c.epoch == new.epoch and c.claim ~= new.claim then return 0 end
end
redis.call('SET', KEYS[1], ARGV[1])
return 1`

// WriteTopology applies the compare-and-set on every instance and returns how
// many accepted it. The write is committed when that reaches the quorum.
func WriteTopology(ctx context.Context, m *store.Majority, t Topology) int {
	b, err := json.Marshal(t)
	if err != nil {
		return 0
	}
	return store.Replied(m.Eval(ctx, casTopology, []string{TopologyKey}, string(b)), 1)
}

// Lease is held by the primary's agent while its node is serving writes.
type Lease struct {
	Node  string `json:"node"`
	Epoch uint64 `json:"epoch"`
}

// renewLease (re)sets the lease unless another node holds it.
const renewLease = `
local cur = redis.call('GET', KEYS[1])
if cur and cjson.decode(cur).node ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])
return 1`

// RenewPrimaryLease extends node's lease by ttl and returns the instances that accepted.
func RenewPrimaryLease(ctx context.Context, m *store.Majority, l Lease, ttl time.Duration) int {
	b, _ := json.Marshal(l)
	return store.Replied(m.Eval(ctx, renewLease, []string{PrimaryLeaseKey}, string(b), l.Node, ttl.Milliseconds()), 1)
}

// PrimaryLeaseAbsent reports whether a majority answered and none of them
// holds the primary lease: nobody can be holding it validly.
func PrimaryLeaseAbsent(ctx context.Context, m *store.Majority) bool {
	return store.Absent(m.Get(ctx, PrimaryLeaseKey)) >= m.Quorum()
}

// acquireLeader takes the lease if it is free or already ours, and extends it.
const acquireLeader = `
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then redis.call('PEXPIRE', KEYS[1], ARGV[2]) return 1 end
if not cur then redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2]) return 1 end
return 0`

const releaseLeader = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0`

// AcquireLeader tries to take or extend the leader lease for id and returns
// how many instances granted it.
func AcquireLeader(ctx context.Context, m *store.Majority, id string, ttl time.Duration) int {
	return store.Replied(m.Eval(ctx, acquireLeader, []string{LeaderKey}, id, ttl.Milliseconds()), 1)
}

// ReleaseLeader drops id's lease wherever it holds it.
func ReleaseLeader(ctx context.Context, m *store.Majority, id string) {
	m.Eval(ctx, releaseLeader, []string{LeaderKey}, id)
}

// Leader returns the lease holder seen on most instances, and on how many.
func Leader(ctx context.Context, m *store.Majority) (string, int) {
	counts := map[string]int{}
	for _, r := range m.Get(ctx, LeaderKey) {
		if r.Err == nil && r.Value != nil {
			counts[string(r.Value)]++
		}
	}
	best, n := "", 0
	for id, c := range counts {
		if c > n {
			best, n = id, c
		}
	}
	return best, n
}

// Failover records one promotion, with the coordinator's clock at each phase.
type Failover struct {
	Epoch  uint64 `json:"epoch"`
	From   string `json:"from"`
	To     string `json:"to,omitempty"`
	By     string `json:"by"`
	Result string `json:"result"` // "promoted" or why it stopped

	SuspectedAt    time.Time `json:"suspected_at"`     // both observers first saw the primary unusable
	LeaseExpiredAt time.Time `json:"lease_expired_at"` // lease gone from a majority, plus margin
	ClaimedAt      time.Time `json:"claimed_at"`       // epoch claimed on a majority
	PreparedAt     time.Time `json:"prepared_at"`      // candidates stopped and caught up
	WritableAt     time.Time `json:"writable_at"`      // new primary open for writes
	PublishedAt    time.Time `json:"published_at"`     // topology names the new primary
}

// PutFailover stores f (no TTL) on every instance.
func PutFailover(ctx context.Context, m *store.Majority, f Failover) {
	b, _ := json.Marshal(f)
	m.Put(ctx, FailoverKey, b, 0)
}

// ReadFailover returns the highest-epoch failover record on any instance.
func ReadFailover(ctx context.Context, m *store.Majority) (Failover, bool) {
	var best Failover
	ok := false
	for _, r := range m.Get(ctx, FailoverKey) {
		var f Failover
		if r.Err != nil || r.Value == nil || json.Unmarshal(r.Value, &f) != nil {
			continue
		}
		if !ok || f.Epoch > best.Epoch {
			best, ok = f, true
		}
	}
	return best, ok
}
