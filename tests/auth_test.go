package tests

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aakashsyadav1999/llmgate/internal/middleware"
)

func TestAuth_MissingHeader(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	handler := middleware.Auth(map[string]bool{"good-key": true})(next)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("next was called despite missing Authorization header")
	}
}

func TestAuth_WrongKey(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	handler := middleware.Auth(map[string]bool{"good-key": true})(next)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
	if called {
		t.Error("next was called despite wrong key")
	}
}

func TestAuth_ValidKey(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	handler := middleware.Auth(map[string]bool{"good-key": true})(next)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer good-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if !called {
		t.Error("next was not called despite a valid key")
	}
}
