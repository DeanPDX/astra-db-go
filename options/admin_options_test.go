// Copyright DataStax, Inc.

package options_test

import (
	"testing"
	"time"

	"github.com/datastax/astra-db-go/options"
)

func TestDropKeyspaceOptionsBuilder(t *testing.T) {
	first := options.DropKeyspace().
		SetBlocking(false).
		SetPollInterval(2 * time.Second)

	second := options.DropKeyspace().SetBlocking(true)

	third := options.DropKeyspace().SetPollInterval(5 * time.Second)

	opts, err := options.MergeOptions(first, second, third)
	if err != nil {
		t.Fatalf("failed to merge options: %v", err)
	}
	if opts == nil {
		t.Fatal("expected non-nil options")
	}

	if opts.Blocking == nil || *opts.Blocking != true {
		t.Errorf("expected Blocking to be true, got %v", opts.Blocking)
	}
	if opts.PollInterval == nil || *opts.PollInterval != 5*time.Second {
		t.Errorf("expected PollInterval to be 5s, got %v", opts.PollInterval)
	}
}
