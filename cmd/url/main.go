// URL engine: go run ./cmd/url
package main

import (
	"time"

	"xray-go/internal/model"
	"xray-go/internal/service"
	"xray-go/internal/service/url"
)

func main() {
	service.RunEngine(model.EngineURL, 1500*time.Millisecond, url.Check)
}
