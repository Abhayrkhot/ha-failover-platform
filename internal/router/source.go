package router

import (
	"context"

	"github.com/Abhayrkhot/ha-failover-platform/internal/cluster"
	"github.com/Abhayrkhot/ha-failover-platform/internal/health"
	"github.com/Abhayrkhot/ha-failover-platform/internal/store"
)

// RedisSource reads the topology and agents' health records from the Redis majority.
type RedisSource struct {
	Store *store.Majority
	Nodes []string
}

// Topology returns the committed topology view.
func (s RedisSource) Topology(ctx context.Context) cluster.View {
	return cluster.ReadTopology(ctx, s.Store)
}

// ReadPool returns nodes whose majority-held record says healthy replica.
func (s RedisSource) ReadPool(ctx context.Context) []string {
	var pool []string
	for _, n := range s.Nodes {
		rec, holders, ok := health.Latest(s.Store.Get(ctx, health.Key(n)))
		if ok && holders >= s.Store.Quorum() && rec.Healthy && rec.Role == health.RoleReplica {
			pool = append(pool, n)
		}
	}
	return pool
}
