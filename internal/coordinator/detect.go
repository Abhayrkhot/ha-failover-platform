package coordinator

import "time"

// evidence is one tick's view of the primary from three independent sources.
type evidence struct {
	// coordDown: the coordinator's own probe failed, or found the primary read-only.
	coordDown bool
	// agentDown: the primary's agent reports it unreachable or read-only, or no
	// Redis majority holds its record (agent dead or cut off).
	agentDown bool
	// leaseAbsent: a Redis majority answered and none holds the primary lease,
	// so the primary has not asserted it is serving for a full lease TTL.
	leaseAbsent bool
}

// detector decides when the primary has failed.
//
// Both observers must agree for failAfter consecutive ticks. Then the lease
// must stay absent for margin: the lease TTL covers the old primary's last
// renewal, and the margin covers TTL expiry happening at slightly different
// times on different Redis instances. The lease wait also confirms the
// failure: a stall shorter than the lease TTL lets the primary's agent renew,
// which resets detection instead of triggering a failover.
type detector struct {
	failAfter int
	margin    time.Duration

	ticks          int
	since          time.Time // first tick both observers agreed
	leaseGoneSince time.Time
}

// observe returns true when a failover should start.
func (d *detector) observe(ev evidence, now time.Time) bool {
	if !ev.coordDown || !ev.agentDown {
		d.reset()
		return false
	}
	if d.ticks == 0 {
		d.since = now
	}
	d.ticks++
	if d.ticks < d.failAfter || !ev.leaseAbsent {
		d.leaseGoneSince = time.Time{}
		return false
	}
	if d.leaseGoneSince.IsZero() {
		d.leaseGoneSince = now
	}
	return now.Sub(d.leaseGoneSince) >= d.margin
}

func (d *detector) reset() {
	d.ticks = 0
	d.since = time.Time{}
	d.leaseGoneSince = time.Time{}
}
