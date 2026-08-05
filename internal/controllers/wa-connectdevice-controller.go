package controllers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"g_backend/internal/service"
)

// WaConnectDeviceController handles HTTP requests for connecting and
// managing a user's WhatsApp devices. The user is always taken from the
// JWT set by the auth middleware (c.GetString("user_id")) — never from a
// request body — so a user can only ever manage devices they own. Which
// device is only ever taken from the URL (:id) and is always checked for
// ownership in the service layer before anything happens to it.
type WaConnectDeviceController struct {
	waService *service.WaConnectDeviceService
}

func NewWaConnectDeviceController(waService *service.WaConnectDeviceService) *WaConnectDeviceController {
	return &WaConnectDeviceController{waService: waService}
}

// deviceIDParam reads the :id route param (a device's UUID), writing a
// 400 response and returning ok=false if it's missing. Device IDs are
// random UUIDs (see models.WaDevice.ID), not sequential integers, so
// there's no numeric parsing to do here — just a presence check.
// Ownership is always re-verified in the service layer regardless.
func deviceIDParam(c *gin.Context) (string, bool) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid device id"})
		return "", false
	}
	return id, true
}

// respondConnectError maps known service errors to HTTP status codes
// shared by every endpoint here that can fail the same ways.
func respondConnectError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrQRTimeout):
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "timed out waiting for QR code, please try again"})
	case errors.Is(err, service.ErrDeviceNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "device not found"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

// ListDevices returns every WhatsApp device the authenticated user owns.
func (wc *WaConnectDeviceController) ListDevices(c *gin.Context) {
	userID := c.GetString("user_id")

	devices, err := wc.waService.ListDevices(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"devices": devices})
}

// AddDevice registers a new WhatsApp device for the authenticated user and
// returns a QR code to scan to pair it.
func (wc *WaConnectDeviceController) AddDevice(c *gin.Context) {
	userID := c.GetString("user_id")

	deviceID, qrCode, status, err := wc.waService.AddDevice(c.Request.Context(), userID)
	if err != nil {
		respondConnectError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"device_id": deviceID,
		"status":    status,
		"qr_string": qrCode,
	})
}

// Status reports one device's current WhatsApp connection status,
// including the latest QR code if one is still pending.
func (wc *WaConnectDeviceController) Status(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}

	status, qrCode, phone, err := wc.waService.Status(userID, deviceID)
	if err != nil {
		respondConnectError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":       status,
		"qr_string":    qrCode,
		"phone_number": phone,
	})
}

// Reconnect (re)starts pairing/connecting for a device the user already
// owns — for a disconnected device, this is how a fresh QR code is
// requested.
func (wc *WaConnectDeviceController) Reconnect(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}

	qrCode, status, err := wc.waService.Reconnect(c.Request.Context(), userID, deviceID)
	if err != nil {
		respondConnectError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    status,
		"qr_string": qrCode,
	})
}

// History returns one device's connection history log (connected,
// disconnected, logged out, reconnect attempts — newest first), so the
// Connect Device page's "Riwayat" view can show why a device dropped
// instead of just its current status.
func (wc *WaConnectDeviceController) History(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}

	history, err := wc.waService.GetDeviceHistory(userID, deviceID)
	if err != nil {
		respondConnectError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"history": history})
}

// Disconnect logs one of the authenticated user's devices out of
// WhatsApp.
func (wc *WaConnectDeviceController) Disconnect(c *gin.Context) {
	userID := c.GetString("user_id")
	deviceID, ok := deviceIDParam(c)
	if !ok {
		return
	}

	if err := wc.waService.Disconnect(c.Request.Context(), userID, deviceID); err != nil {
		respondConnectError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "device disconnected"})
}
