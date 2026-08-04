package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite" // pure-Go sqlite driver, registers as "sqlite"
	"gorm.io/gorm"

	"g_backend/internal/models"
)

var (
	ErrQRTimeout      = errors.New("wa: timed out waiting for a QR code")
	ErrDeviceNotFound = errors.New("wa: device not found")
)

// Presence states exposed to the frontend for a chat contact.
const (
	PresenceOffline = "offline"
	PresenceOnline  = "online"
	PresenceTyping  = "typing"
)

// PresenceInfo is a snapshot of a contact's presence for one chat, kept
// in memory only — WhatsApp presence is inherently ephemeral/live data,
// not something worth persisting to MySQL.
type PresenceInfo struct {
	State    string    `json:"state"`
	LastSeen time.Time `json:"last_seen"`
}

// MessageStore persists messages received over an active WhatsApp
// session. It's implemented by WaInboxService and wired in via
// SetMessageStore after both services are constructed (see
// route.SetupRoutes) — kept as a small local interface, rather than an
// import of WaInboxService, so the two services don't import each other.
type MessageStore interface {
	SaveIncomingMessage(deviceID string, evt *events.Message)

	// HandleHistorySync ingests the batch of past conversations/messages
	// WhatsApp sends shortly after a device is linked (or periodically
	// resyncs), so existing chats show up instead of only ones with new
	// activity after connecting.
	HandleHistorySync(deviceID string, evt *events.HistorySync)
}

// waSession is the live, in-memory state for one connected device: the
// whatsmeow client plus whatever we'd show the frontend right now
// (status, latest QR). Application-level facts that need to survive a
// restart (JID, phone number, last connected time) are mirrored to MySQL
// via WaConnectDeviceService.upsertDevice.
type waSession struct {
	mu     sync.RWMutex
	client *whatsmeow.Client
	status string
	qrCode string
	phone  string

	// cancel stops the background QR-watching goroutine. It is tied to
	// its own long-lived context (NOT the HTTP request context that
	// started the connection), since the goroutine must keep picking up
	// WhatsApp's rotated QR codes long after that request has finished.
	cancel context.CancelFunc
}

func (s *waSession) snapshot() (status, qrCode, phone string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status, s.qrCode, s.phone
}

func (s *waSession) setQR(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.qrCode = code
	s.status = models.WaStatusPendingQR
}

func (s *waSession) setStatus(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

// WaConnectDeviceService owns every WhatsApp device connection. A user
// can own several devices at once ("1 user pasti punya lebih dari 1
// device"), so everything here is keyed by the device's own row ID
// (a random UUID, not an auto-increment integer — see the comment on
// models.WaDevice.ID for why), not by user. WhatsApp session keys are
// persisted by whatsmeow itself in a dedicated SQLite store; connection
// status visible to the rest of the app is mirrored into the
// `wa_devices` table in MySQL.
type WaConnectDeviceService struct {
	db        *gorm.DB
	container *sqlstore.Container
	logger    waLog.Logger

	mu       sync.Mutex
	sessions map[string]*waSession // keyed by WaDevice.ID

	messageStore MessageStore

	presenceMu sync.Mutex
	presence   map[string]map[string]PresenceInfo // deviceID -> chatJID -> info

	// subscribed tracks which (deviceID, chatJID) pairs we've already
	// asked WhatsApp to send presence updates for. WhatsApp presence
	// subscriptions are a live round trip to WhatsApp's servers and stay
	// active for the life of the connection, so re-subscribing on every
	// poll (which used to happen on every ListMessages call, i.e. every
	// few seconds while a chat is open) was pure wasted network calls —
	// a real contributor to the UI feeling heavy/laggy. See
	// EnsurePresenceSubscription.
	subscribedMu sync.Mutex
	subscribed   map[string]map[string]bool // deviceID -> chatJID -> already subscribed
}

// SetMessageStore wires up where incoming messages get persisted. Calling
// this is optional; if it's never called, incoming messages are simply
// not recorded (connection management still works fine on its own).
func (s *WaConnectDeviceService) SetMessageStore(store MessageStore) {
	s.messageStore = store
}

// GetClient returns the live whatsmeow client for a device, if it has an
// active session in this process (connected or still pairing).
func (s *WaConnectDeviceService) GetClient(deviceID string) (*whatsmeow.Client, bool) {
	sess := s.getSession(deviceID)
	if sess == nil {
		return nil, false
	}

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	return sess.client, sess.client != nil
}

// NewWaConnectDeviceService opens (or creates) the whatsmeow SQLite store
// at sqliteDBPath and returns a ready-to-use service.
func NewWaConnectDeviceService(db *gorm.DB, sqliteDBPath string) (*WaConnectDeviceService, error) {
	if dir := filepath.Dir(sqliteDBPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("wa: failed to create storage dir %q: %w", dir, err)
		}
	}

	dbLog := waLog.Stdout("WhatsmeowDB", "WARN", true)
	// WAL mode lets reads proceed without blocking on a writer, and
	// busy_timeout makes SQLite retry for a few seconds on write
	// contention instead of failing instantly with SQLITE_BUSY — both
	// matter here since every connected device shares this one file and
	// can write to it concurrently (incoming messages, prekeys, etc.) as
	// soon as more than one device is connected at the same time.
	dsn := fmt.Sprintf(
		"file:%s?_pragma=foreign_keys(1)&_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)",
		sqliteDBPath,
	)

	container, err := sqlstore.New(context.Background(), "sqlite", dsn, dbLog)
	if err != nil {
		return nil, fmt.Errorf("wa: failed to open whatsmeow store at %q: %w", sqliteDBPath, err)
	}

	return &WaConnectDeviceService{
		db:         db,
		container:  container,
		logger:     waLog.Stdout("WhatsmeowClient", "WARN", true),
		sessions:   make(map[string]*waSession),
		presence:   make(map[string]map[string]PresenceInfo),
		subscribed: make(map[string]map[string]bool),
	}, nil
}

// ListDevices returns every device a user owns, oldest first.
func (s *WaConnectDeviceService) ListDevices(userID string) ([]models.WaDevice, error) {
	var devices []models.WaDevice
	err := s.db.Where("user_id = ?", userID).Order("created_at ASC").Find(&devices).Error
	return devices, err
}

// AddDevice registers a brand new device for a user (with a fresh random
// UUID, assigned by WaDevice.BeforeCreate) and immediately starts pairing
// it, returning the new device's ID together with its first QR code.
func (s *WaConnectDeviceService) AddDevice(ctx context.Context, userID string) (deviceID string, qrCode string, status string, err error) {
	device := models.WaDevice{UserID: userID, Status: models.WaStatusPendingQR}
	if err := s.db.Create(&device).Error; err != nil {
		return "", "", "", fmt.Errorf("wa: failed to create device: %w", err)
	}

	qrCode, status, err = s.connectDevice(ctx, device.ID)
	return device.ID, qrCode, status, err
}

// Reconnect (re)starts pairing/connecting for a device the user already
// owns — typically one that's currently disconnected.
func (s *WaConnectDeviceService) Reconnect(ctx context.Context, userID string, deviceID string) (qrCode string, status string, err error) {
	if err := s.assertOwnership(userID, deviceID); err != nil {
		return "", "", err
	}
	return s.connectDevice(ctx, deviceID)
}

// connectDevice starts (or resumes) the pairing flow for a device.
//   - If a session is already connected, it returns that status with no QR.
//   - If a QR is already pending, it returns the QR already in flight
//     instead of starting a duplicate pairing session.
//   - Otherwise it opens a whatsmeow client, requests a QR code, and
//     returns the first code received (subsequent refreshed codes are only
//     available via Status, since WhatsApp rotates the QR every ~20s).
func (s *WaConnectDeviceService) connectDevice(ctx context.Context, deviceID string) (qrCode string, status string, err error) {
	if sess := s.getSession(deviceID); sess != nil {
		status, qrCode, _ := sess.snapshot()
		return qrCode, status, nil
	}

	device, err := s.loadOrCreateDevice(ctx, deviceID)
	if err != nil {
		return "", "", err
	}

	client := whatsmeow.NewClient(device, s.logger)

	// Deliberately NOT ctx from the HTTP request: that context is
	// canceled the moment this handler returns a response, which would
	// kill QR rotation after the very first code. sessCtx instead lives
	// until we explicitly tear the session down (removeSession).
	sessCtx, cancel := context.WithCancel(context.Background())
	sess := &waSession{client: client, status: models.WaStatusPendingQR, cancel: cancel}

	s.mu.Lock()
	s.sessions[deviceID] = sess
	s.mu.Unlock()

	client.AddEventHandler(s.eventHandler(deviceID))

	// Device already paired from a previous run: just reconnect, no QR.
	if client.Store.ID != nil {
		if err := client.Connect(); err != nil {
			s.removeSession(deviceID)
			return "", "", fmt.Errorf("wa: failed to reconnect existing device: %w", err)
		}
		return "", models.WaStatusConnected, nil
	}

	qrChan, err := client.GetQRChannel(sessCtx)
	if err != nil {
		s.removeSession(deviceID)
		return "", "", fmt.Errorf("wa: failed to open QR channel: %w", err)
	}

	if err := client.Connect(); err != nil {
		s.removeSession(deviceID)
		return "", "", fmt.Errorf("wa: failed to connect: %w", err)
	}

	firstQR := make(chan string, 1)
	go s.watchQRChannel(deviceID, sess, qrChan, firstQR)

	select {
	case code := <-firstQR:
		s.upsertDevice(deviceID, models.WaDevice{Status: models.WaStatusPendingQR})
		return code, models.WaStatusPendingQR, nil
	case <-time.After(15 * time.Second):
		return "", "", ErrQRTimeout
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
}

// watchQRChannel consumes whatsmeow's QR events for one session until the
// device pairs, the QR expires, or the client disconnects.
func (s *WaConnectDeviceService) watchQRChannel(deviceID string, sess *waSession, qrChan <-chan whatsmeow.QRChannelItem, firstQR chan<- string) {
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			sess.setQR(evt.Code)
			select {
			case firstQR <- evt.Code:
			default:
			}
		case "timeout":
			sess.setStatus(models.WaStatusDisconnected)
			s.upsertDevice(deviceID, models.WaDevice{Status: models.WaStatusDisconnected})
			s.removeSession(deviceID)
		case "success":
			// Connection status is handled by events.Connected below;
			// nothing extra to do here.
		}
	}
}

// eventHandler reacts to whatsmeow connection lifecycle events for a
// specific device and mirrors the resulting state into MySQL.
func (s *WaConnectDeviceService) eventHandler(deviceID string) whatsmeow.EventHandler {
	return func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Connected:
			s.markConnected(deviceID)
		case *events.LoggedOut:
			s.markDisconnected(deviceID)
		case *events.Message:
			if s.messageStore != nil {
				s.messageStore.SaveIncomingMessage(deviceID, v)
			}
		case *events.HistorySync:
			if s.messageStore != nil {
				s.messageStore.HandleHistorySync(deviceID, v)
			}
		case *events.Presence:
			state := PresenceOnline
			if v.Unavailable {
				state = PresenceOffline
			}
			s.setPresence(deviceID, v.From.String(), PresenceInfo{State: state, LastSeen: v.LastSeen})
		case *events.ChatPresence:
			state := PresenceOnline
			if v.State == types.ChatPresenceComposing {
				state = PresenceTyping
			}
			s.setPresence(deviceID, v.Chat.String(), PresenceInfo{State: state, LastSeen: time.Now()})
		}
	}
}

func (s *WaConnectDeviceService) markConnected(deviceID string) {
	sess := s.getSession(deviceID)
	if sess == nil {
		return
	}

	var jid, phone string
	if sess.client != nil && sess.client.Store.ID != nil {
		jid = sess.client.Store.ID.String()
		phone = sess.client.Store.ID.User
	}

	sess.mu.Lock()
	sess.status = models.WaStatusConnected
	sess.qrCode = ""
	sess.phone = phone
	sess.mu.Unlock()

	now := time.Now()
	s.upsertDevice(deviceID, models.WaDevice{
		JID:         jid,
		PhoneNumber: phone,
		Status:      models.WaStatusConnected,
		ConnectedAt: &now,
	})
}

func (s *WaConnectDeviceService) markDisconnected(deviceID string) {
	s.removeSession(deviceID)
	s.upsertDevice(deviceID, models.WaDevice{Status: models.WaStatusDisconnected})
}

// Status returns the current connection status for a device, preferring
// the live in-memory session (which has the freshest QR) and falling
// back to the last known state in MySQL if there is no active session
// (e.g. after a server restart).
func (s *WaConnectDeviceService) Status(userID string, deviceID string) (status, qrCode, phone string, err error) {
	if err := s.assertOwnership(userID, deviceID); err != nil {
		return "", "", "", err
	}

	if sess := s.getSession(deviceID); sess != nil {
		status, qrCode, phone = sess.snapshot()
		return status, qrCode, phone, nil
	}

	var device models.WaDevice
	if dbErr := s.db.Where("id = ?", deviceID).First(&device).Error; dbErr != nil {
		return models.WaStatusDisconnected, "", "", nil
	}
	return device.Status, "", device.PhoneNumber, nil
}

// Disconnect logs a device out of WhatsApp and clears its local state.
func (s *WaConnectDeviceService) Disconnect(ctx context.Context, userID string, deviceID string) error {
	if err := s.assertOwnership(userID, deviceID); err != nil {
		return err
	}

	sess := s.getSession(deviceID)
	s.removeSession(deviceID)

	if sess != nil && sess.client != nil {
		sess.client.Logout(ctx)
	}

	return s.upsertDevice(deviceID, models.WaDevice{Status: models.WaStatusDisconnected})
}

// assertOwnership makes sure a device actually belongs to the calling
// user before any status/action call is allowed to touch it.
func (s *WaConnectDeviceService) assertOwnership(userID string, deviceID string) error {
	var device models.WaDevice
	if err := s.db.Where("id = ? AND user_id = ?", deviceID, userID).First(&device).Error; err != nil {
		return ErrDeviceNotFound
	}
	return nil
}

// AssertOwnership is the exported form of assertOwnership, used by
// WaInboxService (and any other service) to check that a device belongs
// to the calling user before touching its chats/messages.
func (s *WaConnectDeviceService) AssertOwnership(userID string, deviceID string) error {
	return s.assertOwnership(userID, deviceID)
}

func (s *WaConnectDeviceService) getSession(deviceID string) *waSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[deviceID]
}

func (s *WaConnectDeviceService) removeSession(deviceID string) {
	s.mu.Lock()
	if sess, ok := s.sessions[deviceID]; ok && sess.cancel != nil {
		sess.cancel()
	}
	delete(s.sessions, deviceID)
	s.mu.Unlock()

	// The presence subscriptions we asked WhatsApp for die with the
	// connection, so drop our "already subscribed" bookkeeping too —
	// otherwise a later reconnect would think it's still subscribed and
	// skip re-subscribing when a chat is reopened.
	s.subscribedMu.Lock()
	delete(s.subscribed, deviceID)
	s.subscribedMu.Unlock()
}

// loadOrCreateDevice reuses a previously paired whatsmeow device for this
// device row if we have one on record, otherwise it allocates a fresh
// whatsmeow device to pair from scratch.
func (s *WaConnectDeviceService) loadOrCreateDevice(ctx context.Context, deviceID string) (*store.Device, error) {
	var waDevice models.WaDevice
	if err := s.db.Where("id = ?", deviceID).First(&waDevice).Error; err != nil {
		return nil, ErrDeviceNotFound
	}

	if waDevice.JID != "" {
		if jid, parseErr := types.ParseJID(waDevice.JID); parseErr == nil {
			if device, getErr := s.container.GetDevice(ctx, jid); getErr == nil && device != nil {
				return device, nil
			}
		}
	}

	return s.container.NewDevice(), nil
}

// upsertDevice updates the wa_devices row for a device with the given
// fields. The row itself always already exists by the time this is
// called (created up front in AddDevice), so this is a plain update, not
// an upsert.
func (s *WaConnectDeviceService) upsertDevice(deviceID string, fields models.WaDevice) error {
	return s.db.Model(&models.WaDevice{}).Where("id = ?", deviceID).Updates(fields).Error
}

// SubscribePresence asks WhatsApp to start sending online/typing updates
// for one chat contact. WhatsApp only sends presence events for JIDs
// you've explicitly subscribed to. Prefer EnsurePresenceSubscription over
// calling this directly — it dedupes so repeated polling doesn't
// re-subscribe (and pay the network round trip) over and over.
func (s *WaConnectDeviceService) SubscribePresence(deviceID string, chatJID string) error {
	client, ok := s.GetClient(deviceID)
	if !ok || client == nil {
		return fmt.Errorf("wa: device is not connected")
	}

	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return fmt.Errorf("wa: invalid chat id: %w", err)
	}

	return client.SubscribePresence(context.Background(), jid)
}

// EnsurePresenceSubscription subscribes to a chat contact's presence only
// the first time it's asked for a given device+chat pair (until the
// device disconnects). WhatsApp presence subscriptions are a live network
// round trip and stay in effect for the life of the connection, so
// calling SubscribePresence on every poll — which is what used to happen
// on every ListMessages call, i.e. every few seconds while a chat stayed
// open — was pure wasted work and a real source of the UI feeling
// heavy/delayed. Failures aren't cached, so a later poll gets to retry.
func (s *WaConnectDeviceService) EnsurePresenceSubscription(deviceID string, chatJID string) {
	s.subscribedMu.Lock()
	if s.subscribed[deviceID] == nil {
		s.subscribed[deviceID] = make(map[string]bool)
	}
	if s.subscribed[deviceID][chatJID] {
		s.subscribedMu.Unlock()
		return
	}
	s.subscribed[deviceID][chatJID] = true
	s.subscribedMu.Unlock()

	if err := s.SubscribePresence(deviceID, chatJID); err != nil {
		s.subscribedMu.Lock()
		delete(s.subscribed[deviceID], chatJID)
		s.subscribedMu.Unlock()
	}
}

// GetPresence returns the last known presence for one chat contact.
// Defaults to offline if nothing has been observed yet (e.g. right after
// subscribing, before WhatsApp has pushed an update).
func (s *WaConnectDeviceService) GetPresence(deviceID string, chatJID string) PresenceInfo {
	s.presenceMu.Lock()
	defer s.presenceMu.Unlock()

	if byChat, ok := s.presence[deviceID]; ok {
		if info, ok := byChat[chatJID]; ok {
			return info
		}
	}
	return PresenceInfo{State: PresenceOffline}
}

func (s *WaConnectDeviceService) setPresence(deviceID string, chatJID string, info PresenceInfo) {
	s.presenceMu.Lock()
	defer s.presenceMu.Unlock()

	if s.presence[deviceID] == nil {
		s.presence[deviceID] = make(map[string]PresenceInfo)
	}
	s.presence[deviceID][chatJID] = info
}
