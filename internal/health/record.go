package health

import (
	"encoding/json"
	"time"

	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// Record is what an agent publishes after every probe. Each copy carries a
// short TTL, so a record that stops being refreshed disappears on its own.
type Record struct {
	Node       string    `json:"node"`
	Boot       string    `json:"boot"` // random per agent process
	Seq        uint64    `json:"seq"`  // increases with every probe of one boot
	ObservedAt time.Time `json:"observed_at"`
	// Role is from the latest successful probe, so an unreachable node keeps
	// the role it last had.
	Role Role `json:"role"`
	Verdict
	Observation Observation `json:"observation"`
}

// Key is the Redis key holding a node's health record.
func Key(node string) string { return "hafo:health:" + node }

// newer reports whether a is a fresher record than b.
func newer(a, b Record) bool {
	if a.Boot == b.Boot {
		return a.Seq > b.Seq
	}
	return a.ObservedAt.After(b.ObservedAt)
}

// Latest picks the freshest record among the copies read from each Redis
// instance. holders is how many instances returned a readable copy; callers
// compare it with the quorum to decide whether the view is trustworthy.
func Latest(results []store.Result) (rec Record, holders int, ok bool) {
	for _, r := range results {
		if r.Err != nil || r.Value == nil {
			continue
		}
		var c Record
		if json.Unmarshal(r.Value, &c) != nil {
			continue
		}
		holders++
		if !ok || newer(c, rec) {
			rec, ok = c, true
		}
	}
	return rec, holders, ok
}
