package controllers

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"g_backend/internal/service"
)

// WaMediaController handles sending and downloading WhatsApp media
// (images, video, audio, documents, stickers) for one of the
// authenticated user's connected devices. Kept in its own file/struct,
// separate from WaInboxController (plain text chat), so each stays small
// and easy to maintain on its own — the two share the same underlying
// WaInboxService, they're just split at the HTTP layer.
type WaMediaController struct {
	inboxService *service.WaInboxService
}

func NewWaMediaController(inboxService *service.WaInboxService) *WaMediaController {
	return &WaMediaController{inboxService: inboxService}
}

// maxUploadSize caps how large a single media upload this endpoint
// accepts. WhatsApp's own clients cap most media well under this; 32MB
// is a practical ceiling that covers photos, short clips, and most
// documents without letting one request buffer something huge into
// memory (the whole file is held in memory while whatsmeow encrypts it
// for upload).
const maxUploadSize = 32 << 20 // 32 MiB

// SendMedia accepts a multipart file upload (field "file", optional
// "caption" and "as_sticker=1") and sends it as a WhatsApp media message
// to one chat.
func (mc *WaMediaController) SendMedia(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}
	chatJID := c.Param("jid")

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxUploadSize)

	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file wajib diupload"})
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "gagal membaca file"})
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "file terlalu besar (maksimal 32MB)"})
		return
	}

	mimeType := fileHeader.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	caption := c.PostForm("caption")
	asSticker := c.PostForm("as_sticker") == "1"

	message, err := mc.inboxService.SendMedia(c.Request.Context(), userID, deviceID, chatJID, data, mimeType, fileHeader.Filename, caption, asSticker)
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

// DownloadMedia streams a previously sent/received media file's bytes
// back to the caller. Ownership is re-checked in the service layer via
// the device the message belongs to, same as every other per-device
// endpoint — the message ID alone isn't treated as proof of access.
func (mc *WaMediaController) DownloadMedia(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}

	messageID, err := strconv.ParseUint(c.Param("messageId"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid message id"})
		return
	}

	path, fileName, mimeType, err := mc.inboxService.GetMediaFile(userID, deviceID, uint(messageID))
	if err != nil {
		if errors.Is(err, service.ErrDeviceNotFound) || errors.Is(err, service.ErrMediaNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "media not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if mimeType != "" {
		c.Header("Content-Type", mimeType)
	}
	if fileName != "" {
		c.Header("Content-Disposition", `inline; filename="`+fileName+`"`)
	}
	c.File(path)
}
