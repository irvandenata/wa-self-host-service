package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeSender struct {
	err   error
	calls int
}

func (f *fakeSender) Send(context.Context, string, string) error { f.calls++; return f.err }

type fakeQueue struct {
	states []string
	acked  bool
}

func (*fakeQueue) Reclaim(context.Context, string) ([]redis.XMessage, error) { return nil, redis.Nil }
func (*fakeQueue) Read(context.Context, string) ([]redis.XMessage, error)    { return nil, redis.Nil }
func (f *fakeQueue) Transition(_ context.Context, _, _, to string, _ int, _ string, _ any) error {
	f.states = append(f.states, to)
	return nil
}
func (f *fakeQueue) Ack(context.Context, string) error { f.acked = true; return nil }

func TestProcessSuccessAndFailure(t *testing.T) {
	message := redis.XMessage{ID: "1-0", Values: map[string]any{"id": "job", "receiver": "6281", "message": "hello"}}

	successQueue, successSender := &fakeQueue{}, &fakeSender{}
	New(successQueue, successSender, 1, 1, 100, time.Second).process(context.Background(), message)
	if successSender.calls != 1 || !successQueue.acked || successQueue.states[len(successQueue.states)-1] != "completed" {
		t.Fatalf("success not completed: %+v", successQueue)
	}

	failureQueue, failureSender := &fakeQueue{}, &fakeSender{err: errors.New("offline")}
	New(failureQueue, failureSender, 1, 1, 100, time.Second).process(context.Background(), message)
	if failureSender.calls != 1 || !failureQueue.acked || failureQueue.states[len(failureQueue.states)-1] != "failed" {
		t.Fatalf("failure not recorded: %+v", failureQueue)
	}
}
