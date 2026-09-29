package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type Manager struct {
	client         *whatsmeow.Client
	store          *sqlstore.Container
	mu             sync.RWMutex
	status         string
	qr             string
	lastError      string
	connecting     bool
	explicitLogout bool
	pairingID      uint64
	ctx            context.Context
	cancel         context.CancelFunc
}

func New(dbPath string) (*Manager, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0750); err != nil {
		return nil, err
	}
	container, err := sqlstore.New(context.Background(), "sqlite3", "file:"+dbPath+"?_foreign_keys=on", nil)
	if err != nil {
		return nil, err
	}
	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		container.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{store: container, client: whatsmeow.NewClient(device, nil), status: "disconnected", ctx: ctx, cancel: cancel}
	m.client.EnableAutoReconnect = true
	m.client.AddEventHandler(m.handleEvent)
	if device.ID != nil {
		slog.Info("WhatsApp session found", "jid", device.ID.String())
	}
	return m, nil
}

func (m *Manager) Start() {
	if m.client.Store.ID != nil {
		_ = m.Connect(context.Background())
	}
}

func (m *Manager) Connect(ctx context.Context) error {
	m.mu.Lock()
	if m.connecting || m.client.IsConnected() {
		m.mu.Unlock()
		return nil
	}
	m.connecting, m.explicitLogout = true, false
	m.status = "connecting"
	m.pairingID++
	pairingID := m.pairingID
	m.mu.Unlock()

	// WhatsApp rejects stale web client revisions. Refreshing the revision at
	// connection time keeps pairing compatible without requiring a library
	// upgrade whenever WhatsApp Web publishes a new build.
	if latestVersion, err := whatsmeow.GetLatestVersion(ctx, &http.Client{Timeout: 10 * time.Second}); err != nil {
		slog.Warn("failed to refresh WhatsApp client version", "error", err)
	} else {
		store.SetWAVersion(*latestVersion)
		slog.Info("WhatsApp client version refreshed", "version", latestVersion.String())
	}

	var qr <-chan whatsmeow.QRChannelItem
	var err error
	if m.client.Store.ID == nil {
		pairingCtx, cancelPairing := context.WithCancel(m.ctx)
		qr, err = m.client.GetQRChannel(pairingCtx)
		if err != nil {
			cancelPairing()
			m.setConnectError(pairingID, err)
			return err
		}
		m.mu.Lock()
		m.status = "pairing"
		m.mu.Unlock()
		slog.Info("QR pairing started")
		go m.consumeQR(pairingID, qr, cancelPairing)
	}
	go func() {
		if err := m.client.Connect(); err != nil {
			m.setConnectError(pairingID, err)
		}
	}()
	return nil
}

func (m *Manager) consumeQR(pairingID uint64, channel <-chan whatsmeow.QRChannelItem, cancel context.CancelFunc) {
	defer cancel()
	defer m.finishPairingChannel(pairingID)

	for item := range channel {
		m.mu.Lock()
		if m.pairingID != pairingID {
			m.mu.Unlock()
			return
		}
		terminal := false
		if item.Event == "code" {
			m.qr, m.status, m.lastError = item.Code, "pairing", ""
		} else if item.Event == "success" {
			m.qr, m.status, m.lastError = "", "connected", ""
			m.connecting = false
			terminal = true
		} else {
			m.qr, m.status, m.connecting = "", "disconnected", false
			m.lastError = item.Event
			if item.Error != nil {
				m.lastError = item.Error.Error()
			}
			terminal = true
		}
		m.mu.Unlock()

		if terminal {
			if item.Event != "success" {
				slog.Error("QR pairing stopped", "event", item.Event, "error", item.Error)
				m.client.Disconnect()
			}
			return
		}
	}
}

func (m *Manager) handleEvent(event any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch event.(type) {
	case *events.Connected:
		m.status, m.qr, m.lastError, m.connecting = "connected", "", "", false
		slog.Info("WhatsApp connected")
	case *events.Disconnected:
		if !m.explicitLogout {
			m.status = "disconnected"
		}
		slog.Warn("WhatsApp disconnected")
	case *events.LoggedOut:
		m.status, m.qr, m.connecting = "logged_out", "", false
		slog.Warn("WhatsApp logged out")
	}
}

func (m *Manager) setConnectError(pairingID uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pairingID != pairingID {
		return
	}
	m.status, m.connecting = "disconnected", false
	m.qr = ""
	m.lastError = err.Error()
	slog.Error("WhatsApp connection failed", "error", err)
}

func (m *Manager) finishPairingChannel(pairingID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pairingID == pairingID && m.connecting {
		m.status, m.qr, m.connecting = "disconnected", "", false
	}
}

func (m *Manager) Status() (string, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	jid := ""
	if m.client.Store.ID != nil {
		jid = m.client.Store.ID.String()
	}
	return m.status, jid
}

func (m *Manager) QR() (string, string) { m.mu.RLock(); defer m.mu.RUnlock(); return m.status, m.qr }

func (m *Manager) LastError() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastError
}

func (m *Manager) Logout(ctx context.Context) error {
	m.mu.Lock()
	m.explicitLogout = true
	m.mu.Unlock()
	if err := m.client.Logout(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.status, m.qr, m.lastError, m.connecting = "logged_out", "", "", false
	m.mu.Unlock()
	return nil
}

func (m *Manager) Send(ctx context.Context, receiver, message string) error {
	if !m.client.IsConnected() || !m.client.IsLoggedIn() {
		return errors.New("whatsapp not connected")
	}
	jid := types.NewJID(receiver, types.DefaultUserServer)
	_, err := m.client.SendMessage(ctx, jid, &waE2E.Message{Conversation: proto.String(message)})
	return err
}

func (m *Manager) Close() error {
	m.cancel()
	m.client.Disconnect()
	if err := m.store.Close(); err != nil {
		return fmt.Errorf("close WhatsApp store: %w", err)
	}
	return nil
}
