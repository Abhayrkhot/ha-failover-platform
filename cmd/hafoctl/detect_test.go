package main

import "testing"

func f(v float64) *float64 { return &v }

func TestSummaryNearestRank(t *testing.T) {
	xs := []float64{10, 1, 9, 2, 8, 3, 7, 4, 6, 5}
	s := summary(xs)
	if s.P50 != 5 || s.P95 != 10 || s.Max != 10 {
		t.Fatalf("got %+v, want p50 5, p95 10, max 10", s)
	}
	if got := summary(nil); got != (stats{}) {
		t.Fatalf("empty input: %+v", got)
	}
}

func TestSummarizeMeasuresFromUnhealthy(t *testing.T) {
	rows := summarize([]trial{
		{Fault: "pause", AgentMS: f(900), AlertMS: f(1500)},
		{Fault: "pause", AgentMS: f(700)},
		{Fault: "applier-stall", UnhealthyAtMS: 800, AgentMS: f(1000), AlertMS: f(1600)},
	})
	if len(rows) != 2 || rows[0].Fault != "pause" || rows[1].Fault != "applier-stall" {
		t.Fatalf("rows not in first-seen order: %+v", rows)
	}
	if rows[0].Agent.Max != 900 || rows[0].AlertMissed != 1 {
		t.Fatalf("pause row: %+v", rows[0])
	}
	if rows[1].Agent.P50 != 200 || rows[1].Alert.P50 != 800 {
		t.Fatalf("stall latencies should subtract unhealthy_at_ms: %+v", rows[1])
	}
}
