package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type InspectRequest struct {
	MessageID   string `json:"message_id"`
	CustomerID  string `json:"customer_id"`
	Body        string `json:"body"`
	CallbackURL string `json:"callback_url"`
}

func main() {
	jobs := make(chan InspectRequest, 1000)
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
			http.Error(w, "calLback_url is required", http.StatusBadRequest)
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
