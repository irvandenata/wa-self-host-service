package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"wa-service/internal/config"
	"wa-service/internal/queue"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	qrcode "github.com/skip2/go-qrcode"
)

//go:embed openapi.yaml
var docs embed.FS

type Queue interface {
	Enqueue(context.Context, queue.Message, string) (string, error)
	Get(context.Context, string) (queue.Job, error)
	Stats(context.Context) (queue.Stats, error)
	Drain(context.Context) error
}

type WhatsApp interface {
	Connect(context.Context) error
	Status() (string, string)
	QR() (string, string)
	LastError() string
	Logout(context.Context) error
}

type Server struct {
	config config.Config
	queue  Queue
	wa     WhatsApp
	ready  func(context.Context) error
}

func New(c config.Config, q Queue, wa WhatsApp, ready func(context.Context) error) http.Handler {
	s := &Server{config: c, queue: q, wa: wa, ready: ready}
	r := chi.NewRouter()
	r.Use(cors)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
	})
	r.Get("/readyz", s.readyz)
	r.Get("/metrics", s.metrics)
	r.Get("/docs", swagger)
	r.Get("/docs/", swagger)
	r.Get("/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		data, _ := docs.ReadFile("openapi.yaml")
		_, _ = w.Write(data)
	})
	r.Route("/v1", func(r chi.Router) {
		r.Use(s.authenticate)
		r.Post("/messages", s.enqueue)
		r.Post("/messages/bulk", s.bulk)
		r.Get("/jobs/{id}", s.job)
		r.Get("/queue/stats", s.stats)
		r.Delete("/queue/drain", s.drain)
		r.Get("/wa/status", s.waStatus)
		r.Post("/wa/connect", s.waConnect)
		r.Get("/wa/qr", s.waQR)
		r.Get("/wa/qr.png", s.waQRImage)
		r.Post("/wa/logout", s.waLogout)
	})
	return r
}

type messageRequest struct {
	Receiver           string `json:"receiver"`
	Message            string `json:"message"`
	EnableTypingEffect *bool  `json:"enableTypingEffect,omitempty"`
	TypingSpeedMS      *int   `json:"typingSpeedMs,omitempty"`
}

type bulkRequest struct {
	Receivers          []string `json:"receivers"`
	Message            string   `json:"message"`
	EnableTypingEffect *bool    `json:"enableTypingEffect,omitempty"`
	TypingSpeedMS      *int     `json:"typingSpeedMs,omitempty"`
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var input messageRequest
	if err := decode(r, &input); err != nil {
		invalid(w, err)
		return
	}
	receiver, err := NormalizePhone(input.Receiver)
	if err != nil || strings.TrimSpace(input.Message) == "" || (input.TypingSpeedMS != nil && *input.TypingSpeedMS < 1) {
		invalid(w, errors.New("receiver and message are required"))
		return
	}
	id, err := s.queue.Enqueue(r.Context(), queue.Message{Receiver: receiver, Text: input.Message}, r.Header.Get("Idempotency-Key"))
	if err != nil {
		jsonError(w, http.StatusServiceUnavailable, "queue_unavailable")
		return
	}
	slog.Info("message queued", "jobId", id, "receiver", maskPhone(receiver))
	jsonResponse(w, http.StatusOK, map[string]any{"enqueued": true, "jobId": id})
}

func (s *Server) bulk(w http.ResponseWriter, r *http.Request) {
	var input bulkRequest
	if err := decode(r, &input); err != nil || len(input.Receivers) == 0 || strings.TrimSpace(input.Message) == "" || (input.TypingSpeedMS != nil && *input.TypingSpeedMS < 1) {
		invalid(w, errors.New("receivers and message are required"))
		return
	}
	ids := make([]string, 0, len(input.Receivers))
	for _, raw := range input.Receivers {
		receiver, err := NormalizePhone(raw)
		if err != nil {
			invalid(w, err)
			return
		}
		id, err := s.queue.Enqueue(r.Context(), queue.Message{Receiver: receiver, Text: input.Message}, r.Header.Get("Idempotency-Key"))
		if err != nil {
			jsonError(w, http.StatusServiceUnavailable, "queue_unavailable")
			return
		}
		ids = append(ids, id)
	}
	jsonResponse(w, http.StatusOK, map[string]any{"enqueued": true, "jobs": ids, "count": len(ids)})
}

func (s *Server) job(w http.ResponseWriter, r *http.Request) {
	job, err := s.queue.Get(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, redis.Nil) {
		jsonError(w, http.StatusNotFound, "not_found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusServiceUnavailable, "queue_unavailable")
		return
	}
	jsonResponse(w, http.StatusOK, job)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.queue.Stats(r.Context())
	if err != nil {
		jsonError(w, http.StatusServiceUnavailable, "queue_unavailable")
		return
	}
	jsonResponse(w, http.StatusOK, stats)
}

func (s *Server) drain(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		jsonError(w, http.StatusForbidden, "forbidden")
		return
	}
	if err := s.queue.Drain(r.Context()); err != nil {
		jsonError(w, http.StatusServiceUnavailable, "queue_unavailable")
		return
	}
	jsonResponse(w, http.StatusOK, map[string]bool{"drained": true})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.ready(ctx); err != nil {
		jsonResponse(w, http.StatusServiceUnavailable, map[string]bool{"ok": false})
		return
	}
	jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	stats, err := s.queue.Stats(r.Context())
	if err != nil {
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "wa_queue_waiting %d\nwa_queue_active %d\nwa_queue_completed %d\nwa_queue_failed %d\nwa_queue_delayed %d\n", stats.Waiting, stats.Active, stats.Completed, stats.Failed, stats.Delayed)
}

func (s *Server) waStatus(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		jsonError(w, 403, "forbidden")
		return
	}
	status, jid := s.wa.Status()
	response := waResponse(status, s.wa.LastError())
	response["jid"] = jid
	jsonResponse(w, 200, response)
}
func (s *Server) waConnect(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		jsonError(w, 403, "forbidden")
		return
	}
	if err := s.wa.Connect(r.Context()); err != nil {
		jsonError(w, 500, "connect_failed")
		return
	}
	status, jid := s.wa.Status()
	response := waResponse(status, s.wa.LastError())
	response["jid"] = jid
	jsonResponse(w, 200, response)
}
func (s *Server) waQR(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		jsonError(w, 403, "forbidden")
		return
	}
	status, qr := s.wa.QR()
	response := waResponse(status, s.wa.LastError())
	response["qr"] = qr
	jsonResponse(w, 200, response)
}
func (s *Server) waQRImage(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		jsonError(w, http.StatusForbidden, "forbidden")
		return
	}
	status, payload := s.wa.QR()
	if payload == "" {
		response := waResponse(status, s.wa.LastError())
		response["error"] = "qr_unavailable"
		jsonResponse(w, http.StatusConflict, response)
		return
	}
	png, err := qrcode.Encode(payload, qrcode.Medium, 512)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "qr_encode_failed")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "image/png")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(png)
}
func (s *Server) waLogout(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		jsonError(w, 403, "forbidden")
		return
	}
	if err := s.wa.Logout(r.Context()); err != nil {
		jsonError(w, 500, "logout_failed")
		return
	}
	jsonResponse(w, 200, map[string]string{"status": "logged_out"})
}

func waResponse(status, lastError string) map[string]string {
	response := map[string]string{"status": status}
	if lastError != "" {
		response["error"] = lastError
	}
	return response
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if !hmac.Equal([]byte(key), []byte(s.config.APIKey)) && (s.config.AdminAPIKey == "" || !hmac.Equal([]byte(key), []byte(s.config.AdminAPIKey))) {
			jsonError(w, 401, "unauthorized")
			return
		}
		if s.config.HMACSecret != "" {
			body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			r.Body = io.NopCloser(bytes.NewReader(body))
			if !verifyHMAC(s.config.HMACSecret, r.Header.Get("X-Timestamp"), body, r.Header.Get("X-Signature")) {
				jsonError(w, 401, "invalid_signature")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) admin(r *http.Request) bool {
	if s.config.AdminAPIKey == "" {
		return false
	}
	return hmac.Equal([]byte(r.Header.Get("X-API-Key")), []byte(s.config.AdminAPIKey))
}

func NormalizePhone(input string) (string, error) {
	input = strings.TrimSpace(input)
	var digits strings.Builder
	for i, runeValue := range input {
		if runeValue >= '0' && runeValue <= '9' {
			digits.WriteRune(runeValue)
			continue
		}
		if runeValue == '+' && i == 0 {
			continue
		}
		if runeValue == ' ' || runeValue == '-' || runeValue == '(' || runeValue == ')' {
			continue
		}
		return "", errors.New("invalid receiver")
	}
	phone := digits.String()
	if strings.HasPrefix(phone, "0") {
		phone = "62" + phone[1:]
	}
	if phone == "" {
		return "", errors.New("invalid receiver")
	}
	return phone, nil
}

func verifyHMAC(secret, timestamp string, body []byte, signature string) bool {
	millis, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || signature == "" || time.Since(time.UnixMilli(millis)).Abs() > 5*time.Minute {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("\n"))
	mac.Write(body)
	provided, err := hex.DecodeString(signature)
	return err == nil && hmac.Equal(mac.Sum(nil), provided)
}

func decode(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON value")
	}
	return nil
}
func invalid(w http.ResponseWriter, err error) {
	jsonResponse(w, 400, map[string]any{"error": "invalid_payload", "details": []map[string]string{{"message": err.Error()}}})
}
func jsonError(w http.ResponseWriter, status int, message string) {
	jsonResponse(w, status, map[string]string{"error": message})
}
func jsonResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, Idempotency-Key, X-Timestamp, X-Signature")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func swagger(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, `<!doctype html><html><head><title>WA Queue Service API</title><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.yaml',dom_id:'#swagger-ui'})</script></body></html>`)
}

func maskPhone(phone string) string {
	if len(phone) < 7 {
		return "***"
	}
	return phone[:2] + "***" + phone[len(phone)-4:]
}
