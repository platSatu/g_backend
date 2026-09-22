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

	// UpdateMessageStatus advances a previously-sent message's delivery
	// status (sent -> delivered -> read/played) as WhatsApp reports back
	// on it — see WaInboxService.UpdateMessageStatus.
	UpdateMessageStatus(deviceID string, evt *events.Receipt)
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

	// sendSlotsMu/sendSlots is a per-device serialization gate: exactly
	// one outbound send (text, poll, or media — see AcquireSendSlot's
	// callers in wa-inbox-service.go/wa-media-service.go) may be in
	// flight for a given device at any instant, no matter how many HTTP
	// requests for it arrive at once. A manual chat send, a scheduled
	// broadcast recipient (App\Jobs\SendScheduledWaMessage on the
	// Laravel side), and an AI Bot/auto-reply all funnel through the
	// same gate here.
	//
	// This is a physical backstop, NOT the anti-ban pacing itself — that
	// job belongs to Laravel's own App\Services\Chat\
	// BroadcastThrottleService, which spaces sends for a device out over
	// time (and, as of the fix that added it to SendAutoReplyMessage/
	// SendAiBotReply too, now covers every send path that matters).
	// Even with correct pacing upstream, network jitter, job retries, or
	// several queue workers processing different jobs for the same
	// device can still let two sends land at this backend in the same
	// instant — without this gate they'd both call client.SendMessage
	// against the same whatsmeow socket at once, which is exactly the
	// kind of burst WhatsApp's anti-spam detection flags devices for.
	// Deliberately never cleaned up in removeSession (unlike `subscribed`
	// above): a stale entry here is just one idle buffered channel per
	// device that's ever connected, bounded by device count, and a
	// device reconnecting later reuses the same slot correctly — safer
	// than deleting it and risking a fresh AcquireSendSlot call handing
	// out a brand-new, unlinked channel while an old send is still
	// holding the previous one.
	sendSlotsMu sync.Mutex
	sendSlots   map[string]chan struct{} // deviceID -> 1-buffered token channel
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

// EnsureConnectedClient returns the live whatsmeow client for a device,
// attempting one reconnect first if a session exists in this process but
// its socket has silently dropped — i.e. client.IsConnected() is false
// even though nothing ever fired events.LoggedOut to flip our tracked
// status to disconnected (which is what the Connect Device page and chat
// sync both still read as "healthy"). Without this, SendMessage/SendMedia
// used to fail outright with "device is not connected" on a device that
// was really just one Connect() call away from working again.
func (s *WaConnectDeviceService) EnsureConnectedClient(deviceID string) (*whatsmeow.Client, error) {
	client, ok := s.GetClient(deviceID)
	if !ok || client == nil {
		return nil, fmt.Errorf("wa: device is not connected")
	}

	if client.IsConnected() {
		return client, nil
	}

	s.logger.Warnf("device %s: client not connected when a send was attempted, retrying Connect()", deviceID)
	s.logHistory(deviceID, models.WaDeviceEventReconnecting, "Terdeteksi tidak terhubung saat mengirim pesan, mencoba menyambung ulang")

	if err := client.Connect(); err != nil {
		s.logger.Errorf("device %s: reconnect attempt failed: %v", deviceID, err)
		s.logHistory(deviceID, models.WaDeviceEventReconnectFailed, err.Error())
		return nil, fmt.Errorf("wa: device is not connected")
	}

	// Connect() returns as soon as the socket dial kicks off; the
	// handshake (and IsConnected() flipping true) can lag a beat behind,
	// so give it a short window instead of failing on that technicality.
	for i := 0; i < 10; i++ {
		if client.IsConnected() {
			s.logger.Infof("device %s: reconnect succeeded", deviceID)
			s.logHistory(deviceID, models.WaDeviceEventReconnected, "")
			return client, nil
		}
		time.Sleep(300 * time.Millisecond)
	}

	s.logger.Errorf("device %s: still not connected after reconnect attempt", deviceID)
	s.logHistory(deviceID, models.WaDeviceEventReconnectFailed, "Percobaan sambung ulang tidak berhasil dalam 3 detik")
	return nil, fmt.Errorf("wa: device is not connected")
}

// logHistory records one entry in a device's connection history log —
// best-effort (a logging failure must never break the connection/send
// flow it's attached to). Read back by GetDeviceHistory, which powers the
// Connect Device page's per-device "Riwayat" view.
func (s *WaConnectDeviceService) logHistory(deviceID, event, detail string) {
	s.db.Create(&models.WaDeviceHistory{
		DeviceID: deviceID,
		Event:    event,
		Detail:   detail,
	})
}

// GetDeviceHistory returns a device's connection history, newest first —
// lets a user see why their device disconnected instead of just a bare
// "Terputus" badge with no explanation.
func (s *WaConnectDeviceService) GetDeviceHistory(userID string, deviceID string) ([]models.WaDeviceHistory, error) {
	if err := s.assertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	var history []models.WaDeviceHistory
	err := s.db.
		Where("device_id = ?", deviceID).
		Order("seq DESC").
		Limit(200).
		Find(&history).Error
	return history, err
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
		sendSlots:  make(map[string]chan struct{}),
	}, nil
}

// sendSlotMaxWait bounds how long AcquireSendSlot will ever wait for a
// device's send slot before giving up, regardless of the caller's own
// ctx. Chosen to stay safely under Laravel's queue "retry_after" (90s by
// default — see config/queue.php's 'database' connection on the Laravel
// side): that's the window after which a queue worker considers a job's
// current attempt "lost" and lets another worker pick the SAME job up
// again. Laravel's own HTTP client to this backend (App\Services\Chat\
// InboxService) sets no request timeout of its own, so without this
// bound, a device with a deep backlog of queued sends could make
// AcquireSendSlot block indefinitely — past retry_after — and end up
// with two workers both convinced they're the one sending a given
// message: the original caller (still legitimately waiting its turn)
// and a second one Laravel dispatched believing the first had died. 25s
// leaves real headroom under the 90s ceiling for everything else a
// caller does before/after this wait (JWT mint, throttle/quota checks,
// the network round trip to WhatsApp itself, DB writes) — this is a
// last-resort ceiling for a device that's genuinely overloaded, not the
// expected wait under normal traffic.
const sendSlotMaxWait = 25 * time.Second

// AcquireSendSlot blocks until the caller holds deviceID's single send
// slot — i.e. until no other outbound send is currently in flight for
// that device — or until ctx is done or sendSlotMaxWait elapses,
// whichever comes first. On success it returns a release func the
// caller MUST call exactly once (typically via defer) as soon as its own
// send attempt finishes, success or failure, so the next queued sender
// can proceed. On failure the returned error is ctx's own error (caller
// gave up / caller's own deadline) or context.DeadlineExceeded (this
// device's queue didn't clear within sendSlotMaxWait) — either way,
// callers surface this as "wa: timed out waiting to send" (see
// wa-inbox-service.go/wa-media-service.go), a distinct, non-retriable-
// forever failure rather than a hang. See the sendSlots field's
// docblock for why this gate exists at all.
func (s *WaConnectDeviceService) AcquireSendSlot(ctx context.Context, deviceID string) (func(), error) {
	ctx, cancel := context.WithTimeout(ctx, sendSlotMaxWait)
	defer cancel()

	slot := s.sendSlot(deviceID)

	select {
	case <-slot:
		var released bool
		return func() {
			if released {
				return
			}
			released = true
			slot <- struct{}{}
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// sendSlot returns deviceID's token channel, creating it — pre-filled
// with one token, i.e. starting out "unlocked" — on first use.
func (s *WaConnectDeviceService) sendSlot(deviceID string) chan struct{} {
	s.sendSlotsMu.Lock()
	defer s.sendSlotsMu.Unlock()

	slot, ok := s.sendSlots[deviceID]
	if !ok {
		slot = make(chan struct{}, 1)
		slot <- struct{}{}
		s.sendSlots[deviceID] = slot
	}

	return slot
}

// maxConcurrentRestores caps how many devices dial WhatsApp's servers at
// the exact same instant during RestoreSessions. Each device still
// restores on its own goroutine (one slow/unreachable device must never
// hold up the rest), but with no cap at all a fleet of, say, 200 devices
// would all reconnect in the very same instant on every process
// restart — a burst pattern with no real-world equivalent (a real phone
// doesn't reconnect in perfect lockstep with 199 others) that's exactly
// the kind of signal WhatsApp's anti-abuse systems are built to notice.
// Capped concurrency plus restoreStaggerDelay spreads that startup burst
// out over a few seconds instead of one instant.
const maxConcurrentRestores = 5

// restoreStaggerDelay is held after each device finishes connecting,
// before that concurrency slot is handed to the next device in line —
// combined with maxConcurrentRestores this throttles the *rate* new
// connections start at (roughly maxConcurrentRestores per this interval)
// without making the whole restore process wait for one device at a
// time.
const restoreStaggerDelay = 800 * time.Millisecond

// RestoreSessions reconnects every device that was last known to be
// "connected" back into this process's in-memory s.sessions map. Call
// once at startup, right after NewWaConnectDeviceService — as its own
// goroutine (`go connectDeviceService.RestoreSessions(ctx)`), since this
// function itself blocks until every device has either reconnected or
// failed, and a slow/unreachable device shouldn't delay the HTTP server
// from accepting requests for everyone else.
//
// Device sessions only ever live in s.sessions — see GetClient's
// docblock — and NewWaConnectDeviceService starts that map empty.
// Nothing else ever repopulates it on its own: ListDevices/Status both
// fall back to the DB's last-known `status` column when there's no
// in-memory session (see Status), so the Connect Device page kept
// showing "Terhubung" after a restart even though sending would
// immediately fail with "wa: device is not connected" (see
// EnsureConnectedClient, which only retries an *existing* session's
// socket — it never creates one from nothing). Before this existed, the
// only way to actually recover was opening the Connect Device page and
// clicking Reconnect for that one device by hand, and nothing prompted
// anyone to do that until a send somewhere had already failed — exactly
// what happened to the Google Form auto-reply feature after a routine
// deploy restart, while the Connect Device page itself still claimed
// everything was fine.
//
// whatsmeow already persists this device's paired credentials to its own
// SQLite store (see loadOrCreateDevice), so this is a plain reconnect —
// no QR scan needed, same as the "already paired" branch inside
// connectDevice(). One goroutine per device so a slow/unreachable one
// can't delay the others or hold up the HTTP server from starting — see
// maxConcurrentRestores/restoreStaggerDelay below for how the *rate*
// those goroutines actually dial WhatsApp is throttled.
func (s *WaConnectDeviceService) RestoreSessions(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Errorf("RestoreSessions: panic recovered: %v", r)
		}
	}()

	var devices []models.WaDevice
	if err := s.db.
		Where("jid IS NOT NULL AND jid != '' AND status = ?", models.WaStatusConnected).
		Find(&devices).Error; err != nil {
		s.logger.Errorf("RestoreSessions: failed to list devices to restore: %v", err)
		return
	}

	if len(devices) == 0 {
		return
	}

	s.logger.Infof("RestoreSessions: restoring %d previously-connected device session(s)...", len(devices))

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentRestores)
	for _, d := range devices {
		wg.Add(1)
		sem <- struct{}{}
		go func(deviceID string) {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Errorf("RestoreSessions: panic recovered while restoring device %s: %v", deviceID, r)
				}
			}()
			defer wg.Done()
			defer func() {
				time.Sleep(restoreStaggerDelay)
				<-sem
			}()
			s.restoreOneSession(ctx, deviceID)
		}(d.ID)
	}
	wg.Wait()

	s.logger.Infof("RestoreSessions: done.")
}

// restoreOneSession is RestoreSessions' per-device worker — best-effort,
// never returns an error, since one device's stale/revoked credentials
// must never stop the rest of the fleet from coming back online.
func (s *WaConnectDeviceService) restoreOneSession(ctx context.Context, deviceID string) {
	// Already has a live session (shouldn't happen this early at
	// startup, but cheap to guard against a double-call) — don't clobber
	// it with a second client for the same device.
	if sess := s.getSession(deviceID); sess != nil {
		return
	}

	device, err := s.loadOrCreateDevice(ctx, deviceID)
	if err != nil {
		s.logger.Warnf("RestoreSessions: device %s: no stored credentials, skipping: %v", deviceID, err)
		return
	}

	if device.ID == nil {
		// loadOrCreateDevice fell through to container.NewDevice() — the
		// JID this row remembered doesn't match any credentials actually
		// in whatsmeow's store anymore (wiped/corrupted store, or a very
		// old row). Nothing to restore; needs a real re-pair via QR.
		s.logger.Warnf("RestoreSessions: device %s: stored JID has no matching credentials, skipping", deviceID)
		return
	}

	client := whatsmeow.NewClient(device, s.logger)

	// Own long-lived context, same reasoning as connectDevice(): this
	// must outlive the startup call that kicks it off. No QR flow runs
	// on this path (the device is already paired), so unlike
	// connectDevice() there's no watchQRChannel goroutine to hand it to
	// — it's kept solely so sess.cancel (used by removeSession) has
	// something to call.
	_, cancel := context.WithCancel(context.Background())
	sess := &waSession{client: client, status: models.WaStatusPendingQR, cancel: cancel}

	s.mu.Lock()
	s.sessions[deviceID] = sess
	s.mu.Unlock()

	client.AddEventHandler(s.eventHandler(deviceID))

	if err := client.Connect(); err != nil {
		s.logger.Warnf("RestoreSessions: device %s: reconnect failed: %v", deviceID, err)
		s.removeSession(deviceID)
		s.logHistory(deviceID, models.WaDeviceEventReconnectFailed, "Gagal disambungkan ulang otomatis saat server restart: "+err.Error())
		s.upsertDevice(deviceID, models.WaDevice{Status: models.WaStatusDisconnected})
		return
	}

	// *events.Connected (see eventHandler) flips the session/DB status to
	// "connected" once the handshake actually completes — deliberately
	// not set here, so a device that's genuinely gone (logged out from
	// the phone while this process was down) doesn't get optimistically
	// marked connected only to flip back a moment later.
	s.logHistory(deviceID, models.WaDeviceEventReconnecting, "Disambungkan ulang otomatis saat server start")
}

// connectionWatchdogInterval is how often StartConnectionWatchdog sweeps
// every live session for a socket that's silently dropped. whatsmeow's
// own EnableAutoReconnect (on by default) already retries a socket that
// drops mid-connection, but it doesn't cover every gap on its own — a
// reconnect attempt that itself failed and gave up, or a drop that never
// even surfaced an events.Disconnected. Left alone, a session stuck in
// that state just sits there silently "Terhubung" in the DB while every
// send against it keeps failing, and nothing notices until a user's
// scheduled message quietly never arrives and they come asking why.
const connectionWatchdogInterval = 2 * time.Minute

// StartConnectionWatchdog runs forever — call it once as its own
// goroutine (`go waService.StartConnectionWatchdog(ctx)`), same pattern
// as RestoreSessions. Every connectionWatchdogInterval it checks each
// session this process currently holds and proactively reconnects any
// whose socket is down but whose tracked status still says "connected",
// instead of waiting for the next send attempt to discover that (see
// EnsureConnectedClient, which stays as the last-resort safety net for
// whatever gap this sweep hasn't caught yet — sends still self-heal even
// if the watchdog's timing missed a particular drop).
func (s *WaConnectDeviceService) StartConnectionWatchdog(ctx context.Context) {
	ticker := time.NewTicker(connectionWatchdogInterval)
	defer ticker.Stop()

	s.logger.Infof("StartConnectionWatchdog: watching every %s for dropped sockets", connectionWatchdogInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						s.logger.Errorf("StartConnectionWatchdog: panic recovered during sweep: %v", r)
					}
				}()
				s.sweepSessions()
			}()
		}
	}
}

// sweepSessions is one pass of the watchdog: every currently-held session
// whose socket is down but is still supposed to be connected gets its
// own reconnect goroutine, so one slow/unreachable device can't hold up
// the sweep (or the next tick) for the rest of the fleet.
func (s *WaConnectDeviceService) sweepSessions() {
	s.mu.Lock()
	deviceIDs := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		deviceIDs = append(deviceIDs, id)
	}
	s.mu.Unlock()

	for _, deviceID := range deviceIDs {
		sess := s.getSession(deviceID)
		if sess == nil || sess.client == nil {
			continue
		}

		// Not yet paired (mid-QR-flow) or already healthy — nothing for
		// the watchdog to do.
		if sess.client.Store.ID == nil || sess.client.IsConnected() {
			continue
		}

		// Only step in for a session that's SUPPOSED to be connected —
		// one that's pending QR, or already flipped to disconnected by a
		// real events.LoggedOut, is not this sweep's job to touch.
		status, _, _ := sess.snapshot()
		if status != models.WaStatusConnected {
			continue
		}

		go func(id string) {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Errorf("watchdog: panic recovered while reconnecting device %s: %v", id, r)
				}
			}()
			if _, err := s.EnsureConnectedClient(id); err != nil {
				s.logger.Errorf("watchdog: device %s: proactive reconnect failed: %v", id, err)
			}
		}(deviceID)
	}
}

// ListDevices returns every device the calling user is allowed to see,
// oldest first:
//
//   - Company owner: every device belonging to their company, across
//     every branch (mirrors Laravel's CompanyContextResolver — an owner
//     is always unrestricted).
//   - Branch-locked member: only devices under their own branch office,
//     same company.
//   - No company context at all (standalone user — most accounts that
//     predate this feature): unchanged, falls back to the original
//     "just this user's own devices" behavior.
func (s *WaConnectDeviceService) ListDevices(userID string) ([]models.WaDevice, error) {
	var devices []models.WaDevice
	query := s.db.Order("created_at ASC")

	ctx := resolveCompanyContext(s.db, userID)

	switch {
	case !ctx.Resolved:
		query = query.Where("user_id = ?", userID)
	case ctx.IsOwner:
		query = query.Where("company_id = ?", ctx.CompanyID)
	default:
		query = query.Where("company_id = ? AND branch_office_id = ?", ctx.CompanyID, ctx.BranchOfficeID)
	}

	err := query.Find(&devices).Error
	return devices, err
}

// AddDevice registers a brand new device for a user (with a fresh random
// UUID, assigned by WaDevice.BeforeCreate) and immediately starts pairing
// it, returning the new device's ID together with its first QR code.
//
// CompanyID/BranchOfficeID are stamped once, here, from the creating
// user's CompanyContext at the moment of creation — see the fields'
// docblock on models.WaDevice for why this isn't re-derived later.
func (s *WaConnectDeviceService) AddDevice(ctx context.Context, userID string) (deviceID string, qrCode string, status string, err error) {
	companyCtx := resolveCompanyContext(s.db, userID)

	device := models.WaDevice{UserID: userID, Status: models.WaStatusPendingQR}

	if companyCtx.Resolved {
		device.CompanyID = &companyCtx.CompanyID

		if companyCtx.BranchOfficeID != "" {
			device.BranchOfficeID = &companyCtx.BranchOfficeID
		}
	}

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
			s.logHistory(deviceID, models.WaDeviceEventReconnectFailed, err.Error())
			return "", "", fmt.Errorf("wa: failed to reconnect existing device: %w", err)
		}
		return "", models.WaStatusConnected, nil
	}

	s.logHistory(deviceID, models.WaDeviceEventPendingQR, "Menunggu pemindaian QR code")

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
		func() {
			defer func() {
				if r := recover(); r != nil {
					s.logger.Errorf("watchQRChannel: panic recovered for device %s: %v", deviceID, r)
				}
			}()
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
		}()
	}
}

// eventHandler reacts to whatsmeow connection lifecycle events for a
// specific device and mirrors the resulting state into MySQL.
func (s *WaConnectDeviceService) eventHandler(deviceID string) whatsmeow.EventHandler {
	return func(evt interface{}) {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Errorf("device %s: panic recovered in event handler (event type %T): %v", deviceID, evt, r)
			}
		}()

		switch v := evt.(type) {
		case *events.Connected:
			s.logger.Infof("device %s: connected", deviceID)
			s.logHistory(deviceID, models.WaDeviceEventConnected, "")
			s.markConnected(deviceID)
		case *events.Disconnected:
			// Not a logout — whatsmeow's own auto-reconnect will usually
			// bring this back on its own. Logged (not treated as
			// disconnected in our tracked status) purely so a silent drop
			// like the one that caused "device is not connected" send
			// failures shows up in `journalctl -u g_backend` and the
			// device's history next time.
			s.logger.Warnf("device %s: socket disconnected (whatsmeow will attempt to reconnect)", deviceID)
			s.logHistory(deviceID, models.WaDeviceEventDisconnected, "Koneksi terputus, mencoba menyambung ulang otomatis")
		case *events.LoggedOut:
			s.logger.Warnf("device %s: logged out (reason: %v)", deviceID, v.Reason)
			s.logHistory(deviceID, models.WaDeviceEventLoggedOut, v.Reason.String())
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
		case *events.Receipt:
			if s.messageStore != nil {
				s.messageStore.UpdateMessageStatus(deviceID, v)
			}
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

	s.logHistory(deviceID, models.WaDeviceEventManualDisconnect, "Diputuskan manual dari halaman Device")

	return s.upsertDevice(deviceID, models.WaDevice{Status: models.WaStatusDisconnected})
}

// DisconnectAll closes every live WhatsApp socket cleanly — used only
// during this process's own graceful shutdown (see cmd/server/main.go's
// signal handling), never in response to a user action. Deliberately
// calls the plain whatsmeow Client.Disconnect() here, NOT Logout() —
// compare with Disconnect (above), which calls Logout on purpose because
// a user clicking "Disconnect" on the Connect Device page wants to fully
// unlink that one device. whatsmeow's Client.Disconnect() only tears
// down the local websocket (no context/error — it's a local operation,
// not a round trip to WhatsApp's servers); the device's pairing itself
// is untouched, so RestoreSessions reconnects every one of these devices
// with the SAME session next time this process starts, no QR re-scan
// needed. Getting this backwards (calling Logout() here instead) would
// force every linked device across every company to re-pair from
// scratch on every deploy/restart — this method exists specifically to
// prevent that.
//
// Same locking pattern as sweepSessions: snapshot the current sessions
// under the lock, then do the actual per-session work after releasing
// it, so this never holds mu for longer than copying a slice needs.
func (s *WaConnectDeviceService) DisconnectAll() {
	s.mu.Lock()
	sessions := make([]*waSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()

	closed := 0
	for _, sess := range sessions {
		if sess == nil || sess.client == nil {
			continue
		}
		if sess.client.IsConnected() {
			sess.client.Disconnect()
			closed++
		}
	}

	s.logger.Infof("DisconnectAll: cleanly closed %d of %d WhatsApp socket(s) for shutdown", closed, len(sessions))
}

// assertOwnership makes sure the calling user is allowed to act on a
// device before any status/action call is allowed to touch it. "Allowed"
// now matches ListDevices' visibility rules, not just literal
// user_id == userID:
//
//   - The device's own creator can always act on it (unchanged from
//     before — covers every standalone-user device, and is also just the
//     common case for company devices).
//   - A company owner can act on any device belonging to their company,
//     regardless of who added it or which branch it's under.
//   - A branch-locked member can act on any device under their own
//     branch, regardless of which specific member of that branch added
//     it — a branch's devices are meant to be managed by whoever's
//     responsible for that branch, not locked to whichever individual
//     happened to click "connect" first.
func (s *WaConnectDeviceService) assertOwnership(userID string, deviceID string) error {
	var device models.WaDevice
	if err := s.db.Where("id = ?", deviceID).First(&device).Error; err != nil {
		return ErrDeviceNotFound
	}

	if device.UserID == userID {
		return nil
	}

	ctx := resolveCompanyContext(s.db, userID)
	if !ctx.Resolved || device.CompanyID == nil || *device.CompanyID != ctx.CompanyID {
		return ErrDeviceNotFound
	}

	if ctx.IsOwner {
		return nil
	}

	if device.BranchOfficeID != nil && *device.BranchOfficeID == ctx.BranchOfficeID && ctx.BranchOfficeID != "" {
		return nil
	}

	return ErrDeviceNotFound
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

// GetPresence returns the last known presence for one chat contact,
// preferring the live in-memory value (freshest) and falling back to
// whatever was last persisted to MySQL — see setPresence's docblock for
// why that fallback matters. Only defaults all the way to Offline with
// no last-seen time if neither source has ever seen anything for this
// chat at all (e.g. right after subscribing, before WhatsApp has pushed
// its first update).
func (s *WaConnectDeviceService) GetPresence(deviceID string, chatJID string) PresenceInfo {
	s.presenceMu.Lock()
	if byChat, ok := s.presence[deviceID]; ok {
		if info, ok := byChat[chatJID]; ok {
			s.presenceMu.Unlock()
			return info
		}
	}
	s.presenceMu.Unlock()

	var chat models.WaChat
	if err := s.db.Where("device_id = ? AND chat_jid = ?", deviceID, chatJID).First(&chat).Error; err == nil && chat.PresenceState != "" {
		info := PresenceInfo{State: chat.PresenceState}
		if chat.LastSeenAt != nil {
			info.LastSeen = *chat.LastSeenAt
		}
		// Cache the DB-sourced value in memory too, so the next poll (a
		// few seconds later, same open chat) doesn't have to hit MySQL
		// again just to read back the same thing — only the FIRST read
		// after a restart pays this cost.
		s.setPresenceMemory(deviceID, chatJID, info)
		return info
	}

	return PresenceInfo{State: PresenceOffline}
}

// setPresence records a fresh presence update from a live whatsmeow
// event, both in memory (for fast reads on every poll) and in MySQL
// (so it survives a g_backend restart). Before this, presence was
// in-memory only — which is exactly why "terakhir dilihat" and the
// online/offline pill used to look permanently stuck: a perfectly good
// LastSeen value would vanish the instant the process restarted, even
// though nothing was actually wrong with the presence subscription
// itself.
func (s *WaConnectDeviceService) setPresence(deviceID string, chatJID string, info PresenceInfo) {
	s.setPresenceMemory(deviceID, chatJID, info)

	// Best-effort: only updates a chat row that already exists (a
	// presence event can in principle arrive before any WaChat row for
	// that contact has been created) — never worth failing over, this
	// is a nice-to-have durability layer, not the source of truth for
	// whether presence tracking itself is working.
	updates := map[string]interface{}{"presence_state": info.State}
	if !info.LastSeen.IsZero() {
		updates["last_seen_at"] = info.LastSeen
	}
	s.db.Model(&models.WaChat{}).
		Where("device_id = ? AND chat_jid = ?", deviceID, chatJID).
		Updates(updates)
}

func (s *WaConnectDeviceService) setPresenceMemory(deviceID string, chatJID string, info PresenceInfo) {
	s.presenceMu.Lock()
	defer s.presenceMu.Unlock()

	if s.presence[deviceID] == nil {
		s.presence[deviceID] = make(map[string]PresenceInfo)
	}
	s.presence[deviceID][chatJID] = info
}
