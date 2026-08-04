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
// An optional ?after_id=<id> query param switches this from "give me the
// recent history" (used when a chat is first opened) to "give me
// whatever's new since message <id>" (used by the frontend's polling,
// so it can append instead of re-fetching/re-rendering everything).
func (ic *WaInboxController) ListMessages(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	chatJID := c.Param("jid")

	var afterID uint
	if raw := c.Query("after_id"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_id"})
			return
		}
		afterID = uint(parsed)
	}

	messages, err := ic.inboxService.ListMessages(userID, deviceID, chatJID, afterID)
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
