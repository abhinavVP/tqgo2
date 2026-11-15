package scheduler

import (
	"testing"
)

func TestConfigValidate(t *testing.T) {
	if err := (Config{BatchSize: 1}).Validate(); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if err := (Config{BatchSize: 0}).Validate(); err == nil {
		t.Fatal("zero batch size was accepted")
	}
}
