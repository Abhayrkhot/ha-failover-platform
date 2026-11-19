// Package health turns probes of a MySQL server into a debounced verdict and
// defines the record each agent publishes to the Redis majority.
package health

import (
	"fmt"
	"time"
)

// Role is what a node is doing, judged from its own MySQL settings.
type Role string

const (
	RolePrimary Role = "primary" // read_only=OFF
	RoleReplica Role = "replica" // read-only, replication source configured
	RoleFenced  Role = "fenced"  // read-only, no source: out of service
	RoleUnknown Role = "unknown" // never reached
)

// Observation is one probe of the local MySQL server.
type Observation struct {
	Reachable  bool   `json:"reachable"`
	ProbeError string `json:"probe_error,omitempty"`

	ServerID      uint32 `json:"server_id,omitempty"`
	ServerUUID    string `json:"server_uuid,omitempty"`
	ReadOnly      bool   `json:"read_only"`
	SuperReadOnly bool   `json:"super_read_only"`
	GTIDExecuted  string `json:"gtid_executed,omitempty"`

	// Source side; meaningful on a writable node.
	SemiSyncSourceEnabled bool `json:"semi_sync_source_enabled"`
	// ConnectedReplicas counts replicas streaming from this node. All are
	// semi-sync, so with none connected no commit can be acknowledged.
	ConnectedReplicas int `json:"connected_replicas"`
	// BlockedWrites counts this agent's heartbeat commits parked waiting for a
	// semi-sync ack: the node is writable but cannot complete commits.
	BlockedWrites int `json:"blocked_writes"`

	// Replica side; meaningful when SourceHost is set.
	SourceHost string `json:"source_host,omitempty"`
	IOState    string `json:"io_state,omitempty"` // ON, OFF, CONNECTING
	IOError    string `json:"io_error,omitempty"`
	SQLState   string `json:"sql_state,omitempty"` // ON, OFF
	SQLError   string `json:"sql_error,omitempty"`
	// LagSeconds is the age of the newest primary heartbeat applied here, or
	// nil when no heartbeat row is visible.
	LagSeconds *float64 `json:"lag_seconds,omitempty"`
}

// Role classifies a reachable observation.
func (o Observation) Role() Role {
	switch {
	case !o.Reachable:
		return RoleUnknown
	case !o.ReadOnly:
		return RolePrimary
	case o.SourceHost != "":
		return RoleReplica
	default:
		return RoleFenced
	}
}

// Reason codes. Codes are stable so the coordinator can act on them; Detail is
// for humans.
const (
	CodeStarting        = "starting"
	CodeUnreachable     = "unreachable"
	CodeRecovering      = "recovering"
	CodeFenced          = "fenced"
	CodeWritableReplica = "writable_with_source"
	CodeSemiSyncOff     = "semi_sync_source_off"
	CodeNoReplicas      = "no_replicas_connected"
	CodeWritesBlocked   = "writes_blocked"
	CodeIOThread        = "io_thread_not_running"
	CodeSQLThread       = "sql_thread_not_running"
	CodeLagUnknown      = "lag_unknown"
	CodeLagHigh         = "lag_high"
)

// Reason explains why a node is not healthy.
type Reason struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

func (r Reason) String() string {
	if r.Detail == "" {
		return r.Code
	}
	return r.Code + ": " + r.Detail
}

// Evaluate lists what is wrong with a single reachable observation. An empty
// result means the observation is clean.
func Evaluate(o Observation, maxLag time.Duration) []Reason {
	var rs []Reason
	switch o.Role() {
	case RoleFenced:
		rs = append(rs, Reason{CodeFenced, "read-only with no replication source"})
	case RolePrimary:
		if o.SourceHost != "" {
			rs = append(rs, Reason{CodeWritableReplica, "writable while replicating from " + o.SourceHost})
		}
		if !o.SemiSyncSourceEnabled {
			rs = append(rs, Reason{CodeSemiSyncOff, "acknowledged commits could be lost on failover"})
		} else if o.ConnectedReplicas == 0 {
			rs = append(rs, Reason{CodeNoReplicas, "no replica can acknowledge commits"})
		}
		if o.BlockedWrites > 0 {
			rs = append(rs, Reason{CodeWritesBlocked, fmt.Sprintf("%d heartbeat commit(s) waiting for a semi-sync ack", o.BlockedWrites)})
		}
	case RoleReplica:
		if o.IOState != "ON" {
			rs = append(rs, Reason{CodeIOThread, threadDetail(o.IOState, o.IOError)})
		}
		if o.SQLState != "ON" {
			rs = append(rs, Reason{CodeSQLThread, threadDetail(o.SQLState, o.SQLError)})
		}
		switch {
		case o.LagSeconds == nil:
			rs = append(rs, Reason{CodeLagUnknown, "no heartbeat row applied"})
		case *o.LagSeconds > maxLag.Seconds():
			rs = append(rs, Reason{CodeLagHigh, fmt.Sprintf("%.3fs > %s", *o.LagSeconds, maxLag)})
		}
	}
	return rs
}

func threadDetail(state, err string) string {
	if state == "" {
		state = "absent"
	}
	if err == "" {
		return state
	}
	return state + " (" + err + ")"
}

// Verdict is the debounced health of a node.
type Verdict struct {
	Healthy bool      `json:"healthy"`
	Reasons []Reason  `json:"reasons,omitempty"`
	Since   time.Time `json:"since"` // when Healthy last changed
}

// Thresholds control how quickly the verdict moves.
type Thresholds struct {
	FailAfter    int           // consecutive failed probes before the node counts as unreachable
	RecoverAfter int           // consecutive clean probes before an unhealthy node is healthy again
	MaxLag       time.Duration // replica lag above this is unhealthy
}

// Tracker folds a stream of observations into a verdict.
//
// A failed probe flips the verdict only after FailAfter in a row, so a single
// dropped packet does not pull a replica out of service. Anything a reachable
// server reports about itself (stopped thread, high lag, blocked commits) is
// already a sustained condition and flips the verdict on the first probe.
// Recovery needs RecoverAfter clean probes in a row, so a flapping node does
// not bounce in and out of the read pool.
type Tracker struct {
	th       Thresholds
	verdict  Verdict
	failures int
	clean    int
}

// NewTracker starts unhealthy: a node is not trusted until it has been seen clean.
func NewTracker(th Thresholds, now time.Time) *Tracker {
	return &Tracker{th: th, verdict: Verdict{Reasons: []Reason{{Code: CodeStarting}}, Since: now}}
}

// Observe records one probe result and returns the updated verdict.
func (t *Tracker) Observe(o Observation, now time.Time) Verdict {
	if !o.Reachable {
		t.clean = 0
		t.failures++
		if t.failures >= t.th.FailAfter {
			t.set(false, []Reason{{CodeUnreachable, o.ProbeError}}, now)
		}
		return t.verdict
	}
	t.failures = 0

	if rs := Evaluate(o, t.th.MaxLag); len(rs) > 0 {
		t.clean = 0
		t.set(false, rs, now)
		return t.verdict
	}

	t.clean++
	if !t.verdict.Healthy && t.clean < t.th.RecoverAfter {
		t.set(false, []Reason{{CodeRecovering, fmt.Sprintf("%d/%d clean probes", t.clean, t.th.RecoverAfter)}}, now)
		return t.verdict
	}
	t.set(true, nil, now)
	return t.verdict
}

func (t *Tracker) set(healthy bool, rs []Reason, now time.Time) {
	if healthy != t.verdict.Healthy {
		t.verdict.Since = now
	}
	t.verdict.Healthy = healthy
	t.verdict.Reasons = rs
}
