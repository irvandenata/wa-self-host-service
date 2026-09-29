package queue

import (
	"context"
	"testing"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestEnqueueIdempotencyStatusAndDrain(t *testing.T) {
	server := miniredis.RunT(t)
	q := &Queue{RDB: redis.NewClient(&redis.Options{Addr: server.Addr()})}
	ctx := context.Background()
	if err := q.Init(ctx); err != nil {
		t.Fatal(err)
	}

	message := Message{Receiver: "628123", Text: "hello"}
	first, err := q.Enqueue(ctx, message, "order-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.Enqueue(ctx, message, "order-1")
	if err != nil || second != first {
		t.Fatalf("idempotency: %q != %q (%v)", second, first, err)
	}
	job, err := q.Get(ctx, first)
	if err != nil || job.State != "waiting" {
		t.Fatalf("job status: %+v, %v", job, err)
	}
	stats, err := q.Stats(ctx)
	if err != nil || stats.Waiting != 1 {
		t.Fatalf("stats: %+v, %v", stats, err)
	}
	if err = q.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = q.Get(ctx, first); err != redis.Nil {
		t.Fatalf("drained job still exists: %v", err)
	}
}
