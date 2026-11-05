// KEYWORD engine: go run ./cmd/keyword
package main

import (
	"time"

	"xray-go/internal/model"
	"xray-go/internal/service"
	"xray-go/internal/service/keyword"
)

func main() {
	service.RunEngine(model.EngineKeyword, 500*time.Millisecond, keyword.Check)
}
