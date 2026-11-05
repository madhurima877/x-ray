// Verdict server: reads engine results from Kafka and decides the final verdict.
package main

import (
	"context"
	"encoding/json"
	"log"

	"xray-go/internal/db"
	"xray-go/internal/kafka"
	"xray-go/internal/model"
	"xray-go/internal/redis"
	"xray-go/internal/repo"
	"xray-go/internal/service/verdict"
)

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

	svc := verdict.New(pg, rdb)
	go svc.FinishLate(ctx)

	kr := kafka.NewReader(model.TopicResults, "verdict")
	log.Println("verdict service started")
	for {
		m, err := kr.ReadMessage(ctx)
		if err != nil {
			log.Println("read:", err)
			continue
		}
		var res model.EngineResult
		if err := json.Unmarshal(m.Value, &res); err != nil {
			log.Println("bad message:", err)
			continue
		}
		svc.Add(ctx, res)
	}
}
