package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"tidepact/wire"
)

// Transport is how a client talks to the server.
type Transport interface {
	Register(ctx context.Context) (string, error)
	Activate(ctx context.Context, clientID string) error
	Push(ctx context.Context, req *wire.PushRequest) (*wire.PushResponse, error)
	Pull(ctx context.Context, after int64, limit int) (*wire.PullResponse, error)
	Snapshot(ctx context.Context) (*wire.SnapshotResponse, error)
}

// HTTPTransport talks to a server started with server.NewHandler.
type HTTPTransport struct {
	BaseURL string
	HTTP    *http.Client
}

func NewHTTPTransport(baseURL string) *HTTPTransport {
	return &HTTPTransport{BaseURL: baseURL, HTTP: http.DefaultClient}
}

func (t *HTTPTransport) Register(ctx context.Context) (string, error) {
	var out wire.RegisterResponse
	err := t.do(ctx, http.MethodPost, "/v1/clients", struct{}{}, &out)
	return out.ClientID, err
}

func (t *HTTPTransport) Activate(ctx context.Context, clientID string) error {
	return t.do(ctx, http.MethodPost, "/v1/clients/"+url.PathEscape(clientID)+"/activate", struct{}{}, nil)
}

func (t *HTTPTransport) Push(ctx context.Context, req *wire.PushRequest) (*wire.PushResponse, error) {
	var out wire.PushResponse
	if err := t.do(ctx, http.MethodPost, "/v1/push", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *HTTPTransport) Pull(ctx context.Context, after int64, limit int) (*wire.PullResponse, error) {
	var out wire.PullResponse
	if err := t.do(ctx, http.MethodGet, fmt.Sprintf("/v1/pull?after=%d&limit=%d", after, limit), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *HTTPTransport) Snapshot(ctx context.Context) (*wire.SnapshotResponse, error) {
	var out wire.SnapshotResponse
	if err := t.do(ctx, http.MethodGet, "/v1/snapshot", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *HTTPTransport) do(ctx context.Context, method, path string, in, out any) error {
	var body *bytes.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	} else {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var we wire.Error
		if err := json.NewDecoder(resp.Body).Decode(&we); err != nil || we.Code == "" {
			return fmt.Errorf("tidepact: %s %s: http %d", method, path, resp.StatusCode)
		}
		return &we
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
