package controllers

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"g_backend/internal/service"
)

// WaInboxController handles HTTP requests for reading and sending chat
// messages on one of the authenticated user's connected WhatsApp devices.
// The device is always taken from the URL (:id) and checked for
// ownership in the service layer, same as WaConnectDeviceController.
type WaInboxController struct {
	inboxService *service.WaInboxService
}

func NewWaInboxController(inboxService *service.WaInboxService) *WaInboxController {
	return &WaInboxController{inboxService: inboxService}
}

func respondInboxError(c *gin.Context, err error) {
	if errors.Is(err, service.ErrDeviceNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "device not found"})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

// ListChats returns one device's conversations.
func (ic *WaInboxController) ListChats(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}

	chats, err := ic.inboxService.ListChats(userID, deviceID)
	if err != nil {
		respondInboxError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"chats": chats})
}

// ListMessages returns the message history for one chat on one device.
// An optional ?after_seq=<seq> query param switches this from "give me
// the recent history" (used when a chat is first opened) to "give me
// whatever's new since message <seq>" (used by the frontend's polling,
// so it can append instead of re-fetching/re-rendering everything).
// Deliberately keyed on each message's Seq (a plain AUTO_INCREMENT
// counter), not its ID (a random UUID with no natural order) — see
// models.WaMessage.
func (ic *WaInboxController) ListMessages(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	chatJID := c.Param("jid")

	var afterSeq uint64
	if raw := c.Query("after_seq"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_seq"})
			return
		}
		afterSeq = parsed
	}

	messages, err := ic.inboxService.ListMessages(userID, deviceID, chatJID, afterSeq)
	if err != nil {
		respondInboxError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"messages": messages})
}

type sendMessageRequest struct {
	Body string `json:"body" binding:"required"`
}

// SendMessage sends a text message to one chat through one of the user's
// connected devices.
func (ic *WaInboxController) SendMessage(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	chatJID := c.Param("jid")

	var req sendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload"})
		return
	}

	message, err := ic.inboxService.SendMessage(c.Request.Context(), userID, deviceID, chatJID, req.Body)
	if err != nil {
		if errors.Is(err, service.ErrDeviceNotFound) {
			respondInboxError(c, err)
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": message})
}

type sendPollRequest struct {
	Question        string   `json:"question" binding:"required"`
	Options         []string `json:"options" binding:"required,min=2"`
	SelectableCount int      `json:"selectable_count"`
}

// SendPoll sends a native WhatsApp poll (survey) to one chat through one
// of the user's connected devices — see WaInboxService.SendPoll's
// docblock for why a poll, specifically, is the interactive message type
// this app supports.
func (ic *WaInboxController) SendPoll(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	chatJID := c.Param("jid")

	var req sendPollRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload — a poll needs a question and at least 2 options"})
		return
	}

	selectableCount := req.SelectableCount
	if selectableCount < 1 {
		selectableCount = 1
	}

	message, err := ic.inboxService.SendPoll(c.Request.Context(), userID, deviceID, chatJID, req.Question, req.Options, selectableCount)
	if err != nil {
		if errors.Is(err, service.ErrDeviceNotFound) {
			respondInboxError(c, err)
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": message})
}

// PollResults returns a poll's question/options plus every voter's
// current selection — powers a CRM-side results/tally view.
func (ic *WaInboxController) PollResults(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	pollMessageID := c.Param("message_id")

	poll, votes, err := ic.inboxService.PollResults(userID, deviceID, pollMessageID)
	if err != nil {
		if errors.Is(err, service.ErrDeviceNotFound) {
			respondInboxError(c, err)
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"poll": poll, "votes": votes})
}

// Presence returns the current online/typing state for one chat contact.
func (ic *WaInboxController) Presence(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	chatJID := c.Param("jid")

	presence, err := ic.inboxService.Presence(userID, deviceID, chatJID)
	if err != nil {
		respondInboxError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"state":     presence.State,
		"last_seen": presence.LastSeen,
	})
}
