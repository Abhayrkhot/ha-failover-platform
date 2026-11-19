package archiver

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPickKeepsTheCurrentSourceWhileItIsHealthy(t *testing.T) {
	tests := []struct {
		current string
		healthy []string
		want    string
	}{
		{"", []string{"mysql-3", "mysql-2"}, "mysql-2"},        // first by name
		{"mysql-3", []string{"mysql-2", "mysql-3"}, "mysql-3"}, // no hopping
		{"mysql-3", []string{"mysql-2"}, "mysql-2"},            // current went unhealthy
		{"mysql-2", nil, ""},                                   // nothing safe to archive
	}
	for _, tt := range tests {
		if got := Pick(tt.current, tt.healthy); got != tt.want {
			t.Errorf("Pick(%q, %v) = %q, want %q", tt.current, tt.healthy, got, tt.want)
		}
	}
}

func TestBinlogFilesSkipsIndexAndMetadata(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"binlog.000010", "binlog.000002", "binlog.index", "source.json"} {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got := BinlogFiles(dir)
	want := []string{filepath.Join(dir, "binlog.000002"), filepath.Join(dir, "binlog.000010")}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
