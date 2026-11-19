package coordinator

import (
	"errors"
	"fmt"
	"sort"
)

// candidate is a replica stopped and caught up for promotion.
type candidate struct {
	node string
	gtid string // gtid_executed after the applier drained the relay log
	err  error  // why it could not be prepared
}

var errNoCandidate = errors.New("no reachable replica could be prepared for promotion")

// choose returns the candidate whose executed GTID set contains every other
// candidate's. Promoting anything else would make another replica hold
// transactions the new primary lacks, which can never be reconciled by
// replication. Ties (equal sets) go to the first node by name, so the result
// is deterministic. subset(a, b) reports whether a is contained in b.
func choose(cands []candidate, subset func(a, b string) (bool, error)) (string, error) {
	var ok []candidate
	for _, c := range cands {
		if c.err == nil {
			ok = append(ok, c)
		}
	}
	if len(ok) == 0 {
		return "", errNoCandidate
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i].node < ok[j].node })
	for _, a := range ok {
		holdsAll := true
		for _, b := range ok {
			if a.node == b.node {
				continue
			}
			in, err := subset(b.gtid, a.gtid)
			if err != nil {
				return "", fmt.Errorf("compare GTIDs: %w", err)
			}
			if !in {
				holdsAll = false
				break
			}
		}
		if holdsAll {
			return a.node, nil
		}
	}
	return "", fmt.Errorf("replicas have diverged: none holds every other's transactions")
}
