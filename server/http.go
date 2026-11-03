package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"tidepact/wire"
)

// NewHandler exposes the store over HTTP/JSON.
func NewHandler(s *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/clients", func(w http.ResponseWriter, r *http.Request) {
		id, err := s.Register(r.Context())
		reply(w, &wire.RegisterResponse{ClientID: id}, err)
	})
	mux.HandleFunc("POST /v1/clients/{id}/activate", func(w http.ResponseWriter, r *http.Request) {
		reply(w, struct{}{}, s.Activate(r.Context(), r.PathValue("id")))
	})
	mux.HandleFunc("POST /v1/push", func(w http.ResponseWriter, r *http.Request) {
		var req wire.PushRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			reply(w, nil, wire.Errorf(wire.CodeBadRequest, "%v", err))
			return
		}
		resp, err := s.Push(r.Context(), &req)
		reply(w, resp, err)
	})
	mux.HandleFunc("GET /v1/pull", func(w http.ResponseWriter, r *http.Request) {
		after, err1 := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		limit, err2 := strconv.Atoi(r.URL.Query().Get("limit"))
		if err1 != nil || err2 != nil {
			reply(w, nil, wire.Errorf(wire.CodeBadRequest, "after and limit must be integers"))
			return
		}
		resp, err := s.Pull(r.Context(), after, limit)
		reply(w, resp, err)
	})
	mux.HandleFunc("GET /v1/snapshot", func(w http.ResponseWriter, r *http.Request) {
		resp, err := s.Snapshot(r.Context())
		reply(w, resp, err)
	})
	return mux
}

func reply(w http.ResponseWriter, v any, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		var we *wire.Error
		if !errors.As(err, &we) {
			we = &wire.Error{Code: wire.CodeInternal, Message: err.Error()}
		}
		w.WriteHeader(statusFor(we.Code))
		_ = json.NewEncoder(w).Encode(we)
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func statusFor(code string) int {
	switch code {
	case wire.CodeBadRequest:
		return http.StatusBadRequest
	case wire.CodeUnknownClient:
		return http.StatusNotFound
	case wire.CodeNotActivated:
		return http.StatusForbidden
	case wire.CodeForked:
		return http.StatusConflict
	case wire.CodeFullRequired:
		return http.StatusGone
	default:
		return http.StatusInternalServerError
	}
}
