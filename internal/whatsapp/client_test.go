package whatsapp

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow"
)

func TestSessionStorePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "whatsapp.db")
	manager, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		t.Fatalf("session database was not persisted: %v", err)
	}
}

func TestPairingChannelResetsAfterTerminalEvent(t *testing.T) {
	manager := &Manager{status: "pairing", connecting: true, pairingID: 1}
	channel := make(chan whatsmeow.QRChannelItem, 2)
	channel <- whatsmeow.QRChannelItem{Event: "code", Code: "qr-payload"}
	channel <- whatsmeow.QRChannelTimeout
	close(channel)

	pairingCtx, cancel := context.WithCancel(context.Background())
	manager.consumeQR(1, channel, cancel)

	if pairingCtx.Err() == nil {
		t.Fatal("pairing context was not canceled")
	}
	status, qr := manager.QR()
	if status != "disconnected" || qr != "" || manager.connecting {
		t.Fatalf("terminal event left stale pairing state: status=%q qr=%q connecting=%v", status, qr, manager.connecting)
	}
}
