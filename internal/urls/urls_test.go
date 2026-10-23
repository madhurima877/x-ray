package urls

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestExtract(t *testing.T) {
	text := "Go to HTTPS://Example.COM:443/a#frag, or www.Test.org. Also bit.ly/x1 and http://example.com:443/a! Not e.g. this."
	want := []string{
		"https://example.com/a",
		"http://www.test.org/",
		"http://bit.ly/x1",
		"http://example.com:443/a",
	}
	if got := Extract(text); !reflect.DeepEqual(got, want) {
		t.Fatalf("Extract = %v\nwant %v", got, want)
	}
}

func TestExtractDedupAndCap(t *testing.T) {
	text := ""
	for i := 0; i < 15; i++ {
		text += "http://a.com/" + string(rune('a'+i)) + " http://a.com/a "
	}
	if got := Extract(text); len(got) != MaxURLs {
		t.Fatalf("got %d URLs, want %d", len(got), MaxURLs)
	}
}

func TestHost(t *testing.T) {
	if h := Host("http://WWW.Evil.example/x"); h != "evil.example" {
		t.Fatalf("Host = %q", h)
	}
}

func TestExpand(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/s", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/mid", http.StatusMovedPermanently) })
	mux.HandleFunc("/mid", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/end", http.StatusFound) })
	mux.HandleFunc("/end", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/loop", http.StatusFound) })

	e := NewExpander(time.Second, 5, true)
	chain, err := e.Expand(context.Background(), srv.URL+"/s")
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 || chain[2] != srv.URL+"/end" {
		t.Fatalf("chain = %v", chain)
	}
	if _, err := e.Expand(context.Background(), srv.URL+"/loop"); err == nil {
		t.Fatal("expected too-many-redirects error")
	}
}

func TestExpandRefusesPrivate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	if _, err := NewExpander(time.Second, 5, false).Expand(context.Background(), srv.URL); err == nil {
		t.Fatal("expected loopback to be refused")
	}
}
