package middleware

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
)

type authCtxKey int

const (
	clientKeyContextKey authCtxKey = iota
)

func keyIsValid(validKeys map[string]bool, key string) bool {
	found := false
	for k := range validKeys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(key)) == 1 {
			found = true
		}
	}
	return found
}

func bearerToken(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	return strings.TrimPrefix(h, prefix), true
}

// Auth requires a valid gateway API key on every request, checked
// against the given set of allowed keys.
func Auth(validKeys map[string]bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := bearerToken(r)
			if !ok || !keyIsValid(validKeys, key) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), clientKeyContextKey, key)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
