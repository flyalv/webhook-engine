package httpserver

import (
	"log/slog"
	"time"
	"webhook-engine/internal/event"

	"github.com/gin-gonic/gin"
)

func NewRouter(logger *slog.Logger, eventHandler *event.Handler) *gin.Engine {
	router := gin.New()

	router.Use(gin.Recovery())
	router.Use(requestLogger(logger))

	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status": "ok",
		})
	})

	router.POST("v1/events", eventHandler.Create)

	return router
}

func requestLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		logger.Info("http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", time.Since(start).String(),
			"client_ip", c.ClientIP(),
		)
	}
}
