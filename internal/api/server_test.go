package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"wa-service/internal/config"
	"wa-service/internal/queue"
)

type fakeQueue struct {
	jobs     map[string]queue.Job
	enqueues int
}

func (f *fakeQueue) Enqueue(_ context.Context, message queue.Message, key string) (string, error) {
	f.enqueues++
	id := "job-1"
	if key == "" && f.enqueues > 1 {
		id = "job-2"
	}
	if f.jobs == nil {
		f.jobs = map[string]queue.Job{}
	}
	f.jobs[id] = queue.Job{ID: id, Name: "send", State: "waiting"}
	return id, nil
}
func (f *fakeQueue) Get(_ context.Context, id string) (queue.Job, error) {
	job, ok := f.jobs[id]
	if !ok {
		return queue.Job{}, redis.Nil
	}
	return job, nil
}
func (f *fakeQueue) Stats(context.Context) (queue.Stats, error) {
	return queue.Stats{Waiting: int64(len(f.jobs))}, nil
}
func (f *fakeQueue) Drain(context.Context) error { f.jobs = map[string]queue.Job{}; return nil }

type fakeWA struct{}

func (fakeWA) Connect(context.Context) error { return nil }
func (fakeWA) Status() (string, string)      { return "disconnected", "" }
func (fakeWA) QR() (string, string)          { return "pairing", "code" }
func (fakeWA) LastError() string             { return "" }
func (fakeWA) Logout(context.Context) error  { return nil }

func TestContract(t *testing.T) {
	q := &fakeQueue{jobs: map[string]queue.Job{}}
	h := New(config.Config{APIKey: "test", AdminAPIKey: "admin"}, q, fakeWA{}, func(context.Context) error { return nil })
	request := func(method, path, key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("X-API-Key", key)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	if got := request(http.MethodPost, "/v1/messages", "", `{}`).Code; got != 401 {
		t.Fatalf("missing key: got %d", got)
	}
	if got := request(http.MethodPost, "/v1/messages", "test", `{}`).Code; got != 400 {
		t.Fatalf("invalid payload: got %d", got)
	}
	w := request(http.MethodPost, "/v1/messages", "test", `{"receiver":"0812-3456-7890","message":"hi"}`)
	if w.Code != 200 {
		t.Fatalf("enqueue: got %d: %s", w.Code, w.Body.String())
	}
	var response map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &response)
	if response["enqueued"] != true || response["jobId"] != "job-1" {
		t.Fatalf("unexpected response: %v", response)
	}
	if got := request(http.MethodGet, "/v1/jobs/job-1", "test", "").Code; got != 200 {
		t.Fatalf("job: got %d", got)
	}
	if got := request(http.MethodGet, "/v1/jobs/missing", "test", "").Code; got != 404 {
		t.Fatalf("missing job: got %d", got)
	}
	if got := request(http.MethodPost, "/v1/messages/bulk", "test", `{"receivers":["6281","6282"],"message":"hi"}`).Code; got != 200 {
		t.Fatalf("bulk: got %d", got)
	}
	if got := request(http.MethodGet, "/v1/queue/stats", "test", "").Code; got != 200 {
		t.Fatalf("stats: got %d", got)
	}
	if got := request(http.MethodDelete, "/v1/queue/drain", "test", "").Code; got != 403 {
		t.Fatalf("drain user: got %d", got)
	}
	if got := request(http.MethodDelete, "/v1/queue/drain", "admin", "").Code; got != 200 {
		t.Fatalf("drain admin: got %d", got)
	}
	qrImage := request(http.MethodGet, "/v1/wa/qr.png", "admin", "")
	if qrImage.Code != http.StatusOK || qrImage.Header().Get("Content-Type") != "image/png" || !strings.HasPrefix(qrImage.Body.String(), "\x89PNG") {
		t.Fatalf("QR image: got %d %q", qrImage.Code, qrImage.Header().Get("Content-Type"))
	}
	if got := request(http.MethodGet, "/healthz", "", "").Code; got != 200 {
		t.Fatalf("health: got %d", got)
	}
}

func TestVerifyHMAC(t *testing.T) {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	body := []byte(`{"receiver":"6281","message":"hi"}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte(timestamp + "\n"))
	mac.Write(body)
	if !verifyHMAC("secret", timestamp, body, hex.EncodeToString(mac.Sum(nil))) {
		t.Fatal("valid signature rejected")
	}
	if verifyHMAC("secret", timestamp, body, "bad") {
		t.Fatal("invalid signature accepted")
	}
}

func TestNormalizePhone(t *testing.T) {
	for input, want := range map[string]string{"08123456789": "628123456789", "+628123456789": "628123456789", "628123456789": "628123456789"} {
		got, err := NormalizePhone(input)
		if err != nil || got != want {
			t.Fatalf("NormalizePhone(%q) = %q, %v", input, got, err)
		}
	}
	if _, err := NormalizePhone("hello"); err == nil {
		t.Fatal("expected invalid phone")
	}
}
