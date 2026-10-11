package event

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	service *Service
	logger  *slog.Logger
}

func NewHandler(service *Service, log *slog.Logger) *Handler {
	return &Handler{service: service, logger: log}
}

type createEventRequest struct {
	EventType      string          `json:"event_type" binding:"required"`
	Payload        json.RawMessage `json:"payload" binding:"required"`
	DestinationURL string          `json:"destination_url" binding:"required"`
}

type createEventResponse struct {
	EventID    string `json:"event_id"`
	DeliveryID string `json:"delivery_id"`
	Status     string `json:"status"`
}

func (h *Handler) Create(c *gin.Context) {
	var req createEventRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid request body",
		})
		return
	}

	req.EventType = strings.TrimSpace(req.EventType)
	req.DestinationURL = strings.TrimSpace(req.DestinationURL)

	if err := validateDestinationURL(req.DestinationURL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "invalid destination_url",
		})
		return
	}

	if req.EventType == "" || len(req.Payload) == 0 || string(req.Payload) == "null" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "event_type and a non-null payload are required",
		})
		return
	}

	result, err := h.service.Create(c.Request.Context(), CreateInput{
		EventType:      req.EventType,
		Payload:        req.Payload,
		DestinationURL: req.DestinationURL,
	})
	if err != nil {
		h.logger.Error("failed to create event",
			"error", err,
			"event_type", req.EventType,
			"path", c.Request.URL.Path,
		)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to create event",
		})
		return
	}

	c.JSON(http.StatusAccepted, createEventResponse{
		EventID:    result.EventID,
		DeliveryID: result.DeliveryID,
		Status:     "pending",
	})
}

func validateDestinationURL(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return fmt.Errorf("invalid URL")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("only http and https URLs are allowed")
	}

	if u.Hostname() == "" || u.User != nil {
		return fmt.Errorf("invalid URL hostname or credentials")
	}

	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("invalid URL port")
		}
	}

	return nil
}
