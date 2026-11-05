// AI engine: go run ./cmd/ai
package main

import (
	"time"

	"xray-go/internal/model"
	"xray-go/internal/service"
	"xray-go/internal/service/ai"
)

func main() {
	service.RunEngine(model.EngineAI, 2000*time.Millisecond, ai.Check)
}
