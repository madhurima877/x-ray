// Command mockrelay stands in for the relay system in local runs: it receives
// verdict callbacks and logs them.
package main

import (
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := ":9000"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	http.HandleFunc("POST /callback", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		log.Printf("verdict: %s", body)
		w.WriteHeader(http.StatusNoContent)
	})
	log.Printf("mock relay listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
