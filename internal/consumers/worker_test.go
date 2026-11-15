package consumers

import (
	"testing"

	"scheduled-pipeline/internal/tasks"
)

func TestNewPoolRequiresPositiveEvenCount(t *testing.T) {
	for _, count := range []int{-2, 0, 1, 3} {
		if _, err := NewPool(count, nil, nil); err == nil {
			t.Fatalf("count %d was accepted", count)
		}
	}
}

func TestPoolSplitsTaskTypesEvenly(t *testing.T) {
	pool, err := NewPool(4, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pool.workers[0].SubbedTo != tasks.SMSCallback || pool.workers[1].SubbedTo != tasks.SMSCallback {
		t.Fatalf("SMS subscriptions = %#v", pool.workers[:2])
	}
	if pool.workers[2].SubbedTo != tasks.EmailCallback || pool.workers[3].SubbedTo != tasks.EmailCallback {
		t.Fatalf("email subscriptions = %#v", pool.workers[2:])
	}
}
