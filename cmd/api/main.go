// API server: POST /inspect
package main

import (
	"context"
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"xray-go/internal/db"
	"xray-go/internal/kafka"
	"xray-go/internal/model"
	"xray-go/internal/redis"
	"xray-go/internal/repo"
	"xray-go/internal/service/api"
)

var svc *api.Service

func main() {
	ctx := context.Background()

	pg, err := db.Connect(ctx)
	if err != nil {
		log.Fatal("postgres: ", err)
	}
	if err := repo.Migrate(ctx, pg); err != nil {
		log.Fatal("migrate: ", err)
	}

	rdb, err := redis.Connect(ctx)
	if err != nil {
		log.Fatal("redis: ", err)
	}

	if err := kafka.CreateTopics(model.TopicRequests, model.TopicResults); err != nil {
		log.Fatal("kafka: ", err)
	}

	svc = &api.Service{DB: pg, Redis: rdb, Kafka: kafka.NewWriter(model.TopicRequests)}

	r := gin.Default()
	r.POST("/inspect", inspect)
	log.Fatal(r.Run(":8080"))
}

func inspect(c *gin.Context) {
	var body struct {
		MsgKey  string `json:"msgKey" binding:"required"`
		Content string `json:"content"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "msgKey is required"})
		return
	}

	verdict, err := svc.Inspect(c.Request.Context(), body.MsgKey, body.Content)
	switch {
	case errors.Is(err, api.ErrTimeout):
		c.JSON(http.StatusGatewayTimeout, verdict)
	case err != nil:
		log.Println("inspect:", err)
		c.JSON(http.StatusServiceUnavailable, verdict)
	default:
		c.JSON(http.StatusOK, verdict)
	}
}
