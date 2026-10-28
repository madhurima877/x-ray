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
	http.HandleFunc("POST /inspect", func(w http.ResponseWriter, r *http.Request) {
		var req InspectRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		w.Write([]byte("OK"))

	})
	if err := http.ListenAndServe(":8080", http.DefaultServeMux); err != nil {
		log.Fatal(err)
	}
}
