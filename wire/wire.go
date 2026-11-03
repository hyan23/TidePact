// Package wire defines the client/server protocol types (JSON over HTTP).
package wire

import (
	"errors"
	"fmt"
)

// Op is a node operation as seen by the server.
type Op int

const (
	OpCreate Op = 1
	OpUpdate Op = 2
	OpDelete Op = 3
)

// Node is one version of a key.
type Node struct {
	Key     string `json:"key"`
	KeyID   string `json:"key_id"`
	Version int64  `json:"version"`
	Op      Op     `json:"op"`
	Value   []byte `json:"value,omitempty"`
}

// PushTxn is one client transaction inside a push. ID is the client's local
// transaction id and is echoed back in the result.
type PushTxn struct {
	ID    int64  `json:"id"`
	Nodes []Node `json:"nodes"`
}

type PushRequest struct {
	ClientID   string    `json:"client_id"`
	PrevPushID string    `json:"prev_push_id"`
	PushID     string    `json:"push_id"`
	Txns       []PushTxn `json:"txns"`
}

type TxnResult struct {
	ID       int64 `json:"id"`
	Accepted bool  `json:"accepted"`
	Seq      int64 `json:"seq,omitempty"` // server_seq, set when accepted
}

type PushResponse struct {
	PushID  string      `json:"push_id"`
	Results []TxnResult `json:"results"`
}

// ServerTxn is an accepted transaction in the server's global log.
type ServerTxn struct {
	Seq    int64  `json:"seq"`
	PushID string `json:"push_id"`
	Nodes  []Node `json:"nodes"`
}

type PullResponse struct {
	Txns []ServerTxn `json:"txns"`
}

type SnapshotNode struct {
	Node
	Seq int64 `json:"seq"`
}

// SnapshotResponse is the full state (live leaf nodes only) as of Seq.
type SnapshotResponse struct {
	Seq   int64          `json:"seq"`
	Nodes []SnapshotNode `json:"nodes"`
}

type RegisterResponse struct {
	ClientID string `json:"client_id"`
}

// Error codes.
const (
	CodeBadRequest    = "bad_request"
	CodeUnknownClient = "unknown_client"
	CodeNotActivated  = "not_activated"
	CodeForked        = "forked"
	CodeFullRequired  = "full_required"
	CodeInternal      = "internal"
)

// Error is a protocol error carried in non-200 responses.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return "tidepact: " + e.Code + ": " + e.Message }

func Errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// IsCode reports whether err is a protocol error with the given code.
func IsCode(err error, code string) bool {
	var we *Error
	return errors.As(err, &we) && we.Code == code
}
