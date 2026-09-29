package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

type Sender interface {
	Send(context.Context, string, string) error
}

type Queue interface {
	Reclaim(context.Context, string) ([]redis.XMessage, error)
	Read(context.Context, string) ([]redis.XMessage, error)
	Transition(context.Context, string, string, string, int, string, any) error
	Ack(context.Context, string) error
}

type Worker struct {
	queue                 Queue
	sender                Sender
	attempts, concurrency int
	limiter               *rate.Limiter
	wg                    sync.WaitGroup
}

func New(q Queue, sender Sender, attempts, concurrency, max int, duration time.Duration) *Worker {
	return &Worker{queue: q, sender: sender, attempts: attempts, concurrency: concurrency, limiter: rate.NewLimiter(rate.Every(duration/time.Duration(max)), 1)}
}

func (w *Worker) Run(ctx context.Context) {
	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go func(consumer string) { defer w.wg.Done(); w.loop(ctx, consumer) }(fmt.Sprintf("worker-%d", i+1))
	}
}

func (w *Worker) Wait() { w.wg.Wait() }

func (w *Worker) loop(ctx context.Context, consumer string) {
	for ctx.Err() == nil {
		messages, err := w.queue.Reclaim(ctx, consumer)
		if err == nil && len(messages) > 0 {
			w.process(ctx, messages[0])
			continue
		}
		messages, err = w.queue.Read(ctx, consumer)
		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				slog.Error("queue read failed", "error", err)
			}
			continue
		}
		if len(messages) > 0 {
			w.process(ctx, messages[0])
		}
	}
}

func (w *Worker) process(ctx context.Context, message redis.XMessage) {
	id := fmt.Sprint(message.Values["id"])
	receiver := fmt.Sprint(message.Values["receiver"])
	text := fmt.Sprint(message.Values["message"])
	if id == "" || receiver == "" || text == "" {
		_ = w.queue.Ack(ctx, message.ID)
		return
	}
	if err := w.queue.Transition(ctx, id, "waiting", "active", 0, "", nil); err != nil {
		return
	}
	var err error
	for attempt := 1; attempt <= w.attempts; attempt++ {
		if err = w.limiter.Wait(ctx); err == nil {
			err = w.sender.Send(ctx, receiver, text)
		}
		if err == nil {
			if err = w.queue.Transition(ctx, id, "active", "completed", attempt-1, "", map[string]any{"success": true, "message": "Message sent"}); err != nil {
				return
			}
			_ = w.queue.Ack(ctx, message.ID)
			slog.Info("message sent", "jobId", id, "receiver", mask(receiver))
			return
		}
		if attempt < w.attempts && !wait(ctx, []time.Duration{3 * time.Second, 10 * time.Second}[min(attempt-1, 1)]) {
			return
		}
	}
	if transitionErr := w.queue.Transition(ctx, id, "active", "failed", w.attempts, err.Error(), nil); transitionErr != nil {
		return
	}
	_ = w.queue.Ack(ctx, message.ID)
	slog.Error("message failed", "jobId", id, "receiver", mask(receiver), "error", err)
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func mask(phone string) string {
	if len(phone) < 7 {
		return "***"
	}
	return phone[:2] + "***" + phone[len(phone)-4:]
}
