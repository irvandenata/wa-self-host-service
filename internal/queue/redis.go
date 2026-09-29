package queue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	Stream = "wa:outgoing"
	Group  = "wa-workers"
)

type Message struct{ Receiver, Text string }

type Job struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	AttemptsMade int    `json:"attemptsMade"`
	FailedReason string `json:"failedReason"`
	ReturnValue  any    `json:"returnValue"`
	State        string `json:"state"`
	Receiver     string `json:"-"`
	Text         string `json:"-"`
	StreamID     string `json:"-"`
}

type Stats struct {
	Waiting   int64 `json:"waiting"`
	Active    int64 `json:"active"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Delayed   int64 `json:"delayed"`
}

type Queue struct{ RDB *redis.Client }

func New(rawURL string) (*Queue, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	return &Queue{RDB: redis.NewClient(opts)}, nil
}

func (q *Queue) Init(ctx context.Context) error {
	if err := q.RDB.Ping(ctx).Err(); err != nil {
		return err
	}
	err := q.RDB.XGroupCreateMkStream(ctx, Stream, Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *Queue) Enqueue(ctx context.Context, message Message, idempotencyKey string) (string, error) {
	id := randomID()
	var idemKey string
	if idempotencyKey != "" {
		sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(idempotencyKey)) + "\x00" + message.Receiver + "\x00" + message.Text))
		idemKey = "wa:idempotency:" + hex.EncodeToString(sum[:])
		created, err := q.RDB.SetNX(ctx, idemKey, id, time.Hour).Result()
		if err != nil {
			return "", err
		}
		if !created {
			return q.RDB.Get(ctx, idemKey).Result()
		}
	}

	streamID, err := q.RDB.XAdd(ctx, &redis.XAddArgs{Stream: Stream, Values: map[string]any{
		"id": id, "receiver": message.Receiver, "message": message.Text,
	}}).Result()
	if err != nil {
		if idemKey != "" {
			_ = q.RDB.Del(ctx, idemKey).Err()
		}
		return "", err
	}
	now := time.Now().UnixMilli()
	_, err = q.RDB.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, jobKey(id), "id", id, "name", "send", "attemptsMade", 0, "failedReason", "", "returnValue", "null", "state", "waiting", "receiver", message.Receiver, "message", message.Text, "streamId", streamID)
		p.ZAdd(ctx, stateKey("waiting"), redis.Z{Score: float64(now), Member: id})
		return nil
	})
	return id, err
}

func (q *Queue) Get(ctx context.Context, id string) (Job, error) {
	values, err := q.RDB.HGetAll(ctx, jobKey(id)).Result()
	if err != nil {
		return Job{}, err
	}
	if len(values) == 0 {
		return Job{}, redis.Nil
	}
	job := Job{ID: values["id"], Name: values["name"], FailedReason: values["failedReason"], State: values["state"], Receiver: values["receiver"], Text: values["message"], StreamID: values["streamId"]}
	fmt.Sscan(values["attemptsMade"], &job.AttemptsMade)
	if raw := values["returnValue"]; raw != "" && raw != "null" {
		_ = json.Unmarshal([]byte(raw), &job.ReturnValue)
	}
	return job, nil
}

func (q *Queue) Transition(ctx context.Context, id, from, to string, attempts int, reason string, result any) error {
	raw := "null"
	if result != nil {
		if b, err := json.Marshal(result); err == nil {
			raw = string(b)
		}
	}
	_, err := q.RDB.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if from != "" {
			p.ZRem(ctx, stateKey(from), id)
		}
		p.ZAdd(ctx, stateKey(to), redis.Z{Score: float64(time.Now().UnixMilli()), Member: id})
		p.HSet(ctx, jobKey(id), "state", to, "attemptsMade", attempts, "failedReason", reason, "returnValue", raw)
		if to == "completed" {
			p.Expire(ctx, jobKey(id), time.Hour)
		}
		if to == "failed" {
			p.Expire(ctx, jobKey(id), time.Hour)
		}
		return nil
	})
	return err
}

func (q *Queue) Ack(ctx context.Context, streamID string) error {
	_, err := q.RDB.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.XAck(ctx, Stream, Group, streamID)
		p.XDel(ctx, Stream, streamID)
		return nil
	})
	return err
}

func (q *Queue) Read(ctx context.Context, consumer string) ([]redis.XMessage, error) {
	streams, err := q.RDB.XReadGroup(ctx, &redis.XReadGroupArgs{Group: Group, Consumer: consumer, Streams: []string{Stream, ">"}, Count: 1, Block: 5 * time.Second}).Result()
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return streams[0].Messages, nil
}

func (q *Queue) Reclaim(ctx context.Context, consumer string) ([]redis.XMessage, error) {
	// ponytail: five-minute reclaim avoids a heartbeat; add lease renewal if sends exceed this.
	messages, _, err := q.RDB.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: Stream, Group: Group, Consumer: consumer, MinIdle: 5 * time.Minute, Start: "0-0", Count: 1}).Result()
	return messages, err
}

func (q *Queue) Stats(ctx context.Context) (Stats, error) {
	now := time.Now()
	q.RDB.ZRemRangeByScore(ctx, stateKey("completed"), "-inf", fmt.Sprint(now.Add(-time.Minute).UnixMilli()))
	q.RDB.ZRemRangeByScore(ctx, stateKey("failed"), "-inf", fmt.Sprint(now.Add(-time.Hour).UnixMilli()))
	pipe := q.RDB.Pipeline()
	commands := []*redis.IntCmd{
		pipe.ZCard(ctx, stateKey("waiting")), pipe.ZCard(ctx, stateKey("active")),
		pipe.ZCard(ctx, stateKey("completed")), pipe.ZCard(ctx, stateKey("failed")),
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return Stats{}, err
	}
	return Stats{Waiting: commands[0].Val(), Active: commands[1].Val(), Completed: commands[2].Val(), Failed: commands[3].Val()}, nil
}

func (q *Queue) Drain(ctx context.Context) error {
	ids, err := q.RDB.ZRange(ctx, stateKey("waiting"), 0, -1).Result()
	if err != nil {
		return err
	}
	for _, id := range ids {
		job, getErr := q.Get(ctx, id)
		if getErr != nil && !errors.Is(getErr, redis.Nil) {
			return getErr
		}
		pipe := q.RDB.Pipeline()
		if job.StreamID != "" {
			pipe.XDel(ctx, Stream, job.StreamID)
		}
		pipe.Del(ctx, jobKey(id))
		pipe.ZRem(ctx, stateKey("waiting"), id)
		if _, err = pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func randomID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func jobKey(id string) string      { return "wa:job:" + id }
func stateKey(state string) string { return "wa:jobs:" + state }
