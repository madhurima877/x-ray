package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type InspectRequest struct {
	MessageID   string `json:"message_id"`
	CustomerID  string `json:"customer_id"`
	Body        string `json:"body"`
	CallbackURL string `json:"callback_url"`
}

func main() {
	jobs := make(chan InspectRequest, 10)

	for i := 1; i <= 4; i++ {
		go Worker(i, jobs)
	}
	http.HandleFunc("POST /inspect", func(w http.ResponseWriter, r *http.Request) {
		var req InspectRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if req.MessageID == "" {
			http.Error(w, "message_id is required", http.StatusBadRequest)
			return
		}
		if req.CustomerID == "" {
			http.Error(w, "customer_id is required", http.StatusBadRequest)
			return
		}
		if req.CallbackURL == "" {
			http.Error(w, "callback_url is required", http.StatusBadRequest)
			return
		}
		if req.Body == "" {
			http.Error(w, "body is required", http.StatusBadRequest)
			return
		}
		select {
		case jobs <- req:
		default:
			http.Error(w, "FAIL_OPEN", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(req.MessageID))

	})
	if err := http.ListenAndServe(":8080", http.DefaultServeMux); err != nil {
		log.Fatal(err)
	}
}
func Worker(id int, jobs <-chan InspectRequest) {
	for job := range jobs {
		ctx, cancel := context.WithTimeout(context.Background(), 4500*time.Millisecond)

		select {
		case <-time.After(2 * time.Second):
			log.Printf("worker %d got %s ALLOWED", id, job.MessageID)
		case <-ctx.Done():
			log.Printf("worker %d got %s FAIL_OPEN timeout", id, job.MessageID)
		}
		cancel()
	}

}
