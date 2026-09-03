// Package httpx provides the HTTP building blocks shared by every Fluxgate
// service: a fallible handler signature, a single error envelope, request
// decoding with actionable diagnostics, middleware, and a server that drains
// cleanly.
package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jon-jc/fluxgate/internal/observability"
)

// Handler is an http.Handler that may fail.
//
// Returning an error instead of writing one keeps error rendering in exactly
// one place: handlers describe what went wrong, and the adapter decides the
// status code, the wire format, and what is safe to show a client.
type Handler func(w http.ResponseWriter, r *http.Request) error

// ServeHTTP implements http.Handler.
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h(w, r); err != nil {
		WriteError(w, r, err)
	}
}

// FieldError describes a single validation failure against a request field.
type FieldError struct {
	// Field is a dotted path into the request body, e.g. "points.3.value".
	Field string `json:"field"`
	// Message explains what the client must change.
	Message string `json:"message"`
}

// Problem is an RFC 9457 problem details document. It is the only error shape
// this API ever returns, so clients need exactly one branch to handle failure.
type Problem struct {
	Type      string       `json:"type"`
	Title     string       `json:"title"`
	Status    int          `json:"status"`
	Detail    string       `json:"detail,omitempty"`
	Code      string       `json:"code"`
	RequestID string       `json:"request_id,omitempty"`
	Errors    []FieldError `json:"errors,omitempty"`
}

// Stable, machine-readable error codes. Clients branch on these rather than on
// prose, so they must not change once published.
const (
	CodeBadRequest       = "bad_request"
	CodeValidation       = "validation_failed"
	CodeUnauthorized     = "unauthorized"
	CodeForbidden        = "forbidden"
	CodeNotFound         = "not_found"
	CodeMethodNotAllowed = "method_not_allowed"
	CodeConflict         = "conflict"
	CodePayloadTooLarge  = "payload_too_large"
	CodeUnsupportedMedia = "unsupported_media_type"
	CodeRateLimited      = "rate_limited"
	CodeTimeout          = "timeout"
	CodeUnavailable      = "service_unavailable"
	CodeInternal         = "internal_error"
)

// problemBase namespaces the machine-readable type URIs.
const problemBase = "https://docs.fluxgate.dev/errors/"

// Error is an API error carrying everything needed to render a Problem, plus
// an optional cause that is logged but never sent to the client.
type Error struct {
	// Status is the HTTP status code to return.
	Status int
	// Code is the stable machine-readable identifier.
	Code string
	// Message is safe to show to the client.
	Message string
	// Fields carries per-field validation failures.
	Fields []FieldError
	// Retryable hints that the client may retry the identical request.
	Retryable bool

	// cause is the internal error. It is logged, never serialised.
	cause error
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap exposes the internal cause to errors.Is and errors.As.
func (e *Error) Unwrap() error { return e.cause }

// WithCause attaches an internal cause for logging and returns the error, so
// it can be used inline in a return statement.
func (e *Error) WithCause(err error) *Error {
	e.cause = err
	return e
}

// WithFields attaches validation detail and returns the error.
func (e *Error) WithFields(fields ...FieldError) *Error {
	e.Fields = append(e.Fields, fields...)
	return e
}

// Constructors for the errors handlers actually raise. Each returns a fresh
// value so callers can decorate it without mutating shared state.

// BadRequest reports a malformed request.
func BadRequest(msg string) *Error {
	return &Error{Status: http.StatusBadRequest, Code: CodeBadRequest, Message: msg}
}

// Invalid reports a semantically invalid request body.
func Invalid(msg string, fields ...FieldError) *Error {
	return &Error{
		Status:  http.StatusUnprocessableEntity,
		Code:    CodeValidation,
		Message: msg,
		Fields:  fields,
	}
}

// Unauthorized reports missing or invalid credentials.
func Unauthorized(msg string) *Error {
	return &Error{Status: http.StatusUnauthorized, Code: CodeUnauthorized, Message: msg}
}

// Forbidden reports valid credentials without sufficient permission.
func Forbidden(msg string) *Error {
	return &Error{Status: http.StatusForbidden, Code: CodeForbidden, Message: msg}
}

// NotFound reports a missing resource.
func NotFound(msg string) *Error {
	return &Error{Status: http.StatusNotFound, Code: CodeNotFound, Message: msg}
}

// MethodNotAllowed reports that the path exists but not for this verb. Callers
// must also set an Allow header, which RFC 9110 requires on a 405.
func MethodNotAllowed(msg string) *Error {
	return &Error{
		Status:  http.StatusMethodNotAllowed,
		Code:    CodeMethodNotAllowed,
		Message: msg,
	}
}

// Conflict reports a state conflict, such as a duplicate idempotency key used
// with a different payload.
func Conflict(msg string) *Error {
	return &Error{Status: http.StatusConflict, Code: CodeConflict, Message: msg}
}

// RateLimited reports that the caller exceeded its quota.
func RateLimited(msg string) *Error {
	return &Error{
		Status:    http.StatusTooManyRequests,
		Code:      CodeRateLimited,
		Message:   msg,
		Retryable: true,
	}
}

// Unavailable reports that the service is shedding load or a dependency is
// down. It is always retryable.
func Unavailable(msg string) *Error {
	return &Error{
		Status:    http.StatusServiceUnavailable,
		Code:      CodeUnavailable,
		Message:   msg,
		Retryable: true,
	}
}

// Internal wraps an unexpected failure. The cause is logged; the client only
// ever sees a generic message.
func Internal(cause error) *Error {
	return &Error{
		Status:  http.StatusInternalServerError,
		Code:    CodeInternal,
		Message: "The server encountered an unexpected condition.",
		cause:   cause,
	}
}

// WriteJSON serialises v as JSON with the given status code.
//
// The payload is marshalled into a buffer before any byte reaches the wire: a
// marshalling failure halfway through would otherwise leave the client with a
// 200 status and a truncated body, which is far harder to debug than a clean
// 500.
func WriteJSON(w http.ResponseWriter, r *http.Request, status int, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return Internal(fmt.Errorf("encode response: %w", err))
	}

	h := w.Header()
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/json; charset=utf-8")
	}
	w.WriteHeader(status)

	// A write failure here means the client disconnected. There is no way to
	// report it to them, so record it for the access log and move on.
	if _, err := w.Write(buf.Bytes()); err != nil {
		observability.LoggerFromContext(r.Context()).
			Debug("response write failed", slog.Any("error", err))
	}
	return nil
}

// NoContent replies 204 with no body.
func NoContent(w http.ResponseWriter) error {
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// WriteError renders err as a problem document, translating the errors that
// the runtime and the standard library raise into the right status codes.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	apiErr := asAPIError(r.Context(), err)

	log := observability.LoggerFromContext(r.Context())
	if apiErr.Status >= http.StatusInternalServerError {
		// Only server-side faults are worth an error-level record; a wall of
		// 404s from a misbehaving client is noise that buries real incidents.
		log.Error("request failed",
			slog.String("code", apiErr.Code),
			slog.Int("status", apiErr.Status),
			slog.Any("error", err))
	} else {
		log.Debug("request rejected",
			slog.String("code", apiErr.Code),
			slog.Int("status", apiErr.Status),
			slog.Any("error", err))
	}

	// A cancelled request has no client left to answer. Writing a response
	// would only pollute metrics with a status nobody received.
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		return
	}

	if apiErr.Retryable && w.Header().Get("Retry-After") == "" {
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")

	problem := Problem{
		Type:      problemBase + apiErr.Code,
		Title:     http.StatusText(apiErr.Status),
		Status:    apiErr.Status,
		Detail:    apiErr.Message,
		Code:      apiErr.Code,
		RequestID: RequestIDFromContext(r.Context()),
		Errors:    apiErr.Fields,
	}

	var buf bytes.Buffer
	if encErr := json.NewEncoder(&buf).Encode(problem); encErr != nil {
		// The problem document is built from fixed-shape data, so this cannot
		// realistically fail -- but never leave the client hanging.
		http.Error(w, `{"code":"internal_error"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write(buf.Bytes())
}

// asAPIError maps any error onto the *Error the client will see.
func asAPIError(ctx context.Context, err error) *Error {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return apiErr
	}

	var maxBytes *http.MaxBytesError
	switch {
	case errors.As(err, &maxBytes):
		return &Error{
			Status: http.StatusRequestEntityTooLarge,
			Code:   CodePayloadTooLarge,
			Message: fmt.Sprintf(
				"Request body exceeds the %d byte limit.", maxBytes.Limit),
		}

	case errors.Is(err, context.DeadlineExceeded):
		// Distinguish "we ran out of time" from "the client hung up": only the
		// former is the server's fault and worth retrying.
		return &Error{
			Status:    http.StatusGatewayTimeout,
			Code:      CodeTimeout,
			Message:   "The request exceeded its processing deadline.",
			Retryable: true,
			cause:     err,
		}

	case errors.Is(err, context.Canceled) && ctx.Err() != nil:
		return &Error{
			Status:  499, // nginx's "client closed request"; never sent, only logged.
			Code:    CodeBadRequest,
			Message: "The client closed the connection.",
			cause:   err,
		}
	}

	return Internal(err)
}

// RequireJSONContentType rejects a request that declares a non-JSON body.
//
// An absent Content-Type is tolerated: plenty of minimal clients omit it, and
// the decoder will reject the body anyway if it is not actually JSON.
func RequireJSONContentType(r *http.Request) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
		return &Error{
			Status:  http.StatusUnsupportedMediaType,
			Code:    CodeUnsupportedMedia,
			Message: "Content-Type must be application/json.",
		}
	}
	return nil
}

// ReadBody reads the entire request body, enforcing maxBytes.
//
// Handlers that need the raw bytes -- to fingerprint a payload for
// idempotency, say -- use this and then UnmarshalJSON, rather than decoding
// from the stream and losing the original representation.
func ReadBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// MaxBytesError is mapped to 413 by asAPIError; anything else here is
		// a truncated or aborted upload.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, err
		}
		return nil, BadRequest("Request body could not be read.").WithCause(err)
	}
	return body, nil
}

// UnmarshalJSON decodes data into dst, rejecting anything ambiguous.
//
// Unknown fields are an error rather than a silent no-op: a client that sends
// "streem_id" deserves to hear about the typo immediately instead of debugging
// why its data never arrived. The diagnostics name the offending field and
// byte offset so the caller can fix the payload without guesswork.
func UnmarshalJSON(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return decodeError(err)
	}

	// Reject trailing content so that two concatenated documents cannot be
	// silently truncated to the first one.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return BadRequest("Request body must contain exactly one JSON document.")
	}
	return nil
}

// DecodeJSON reads a JSON request body into dst.
func DecodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	if err := RequireJSONContentType(r); err != nil {
		return err
	}

	body, err := ReadBody(w, r, maxBytes)
	if err != nil {
		return err
	}
	return UnmarshalJSON(body, dst)
}

func decodeError(err error) error {
	var (
		syntaxErr *json.SyntaxError
		typeErr   *json.UnmarshalTypeError
		maxBytes  *http.MaxBytesError
	)

	switch {
	case errors.As(err, &syntaxErr):
		return BadRequest(fmt.Sprintf(
			"Request body contains malformed JSON at byte %d.", syntaxErr.Offset)).
			WithCause(err)

	case errors.As(err, &typeErr):
		field := typeErr.Field
		if field == "" {
			field = "(root)"
		}
		return Invalid("Request body contains a field of the wrong type.",
			FieldError{
				Field: field,
				Message: fmt.Sprintf("expected %s, got %s",
					typeErr.Type.String(), typeErr.Value),
			}).WithCause(err)

	case errors.As(err, &maxBytes):
		return err // asAPIError renders this as 413.

	case errors.Is(err, io.EOF):
		return BadRequest("Request body must not be empty.").WithCause(err)

	case errors.Is(err, io.ErrUnexpectedEOF):
		return BadRequest("Request body contains truncated JSON.").WithCause(err)

	case strings.HasPrefix(err.Error(), "json: unknown field "):
		field := strings.Trim(
			strings.TrimPrefix(err.Error(), "json: unknown field "), `"`)
		return Invalid("Request body contains an unrecognised field.",
			FieldError{Field: field, Message: "unknown field"}).WithCause(err)
	}

	return BadRequest("Request body could not be decoded.").WithCause(err)
}

func isJSONContentType(ct string) bool {
	// Ignore parameters such as "; charset=utf-8".
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	ct = strings.ToLower(strings.TrimSpace(ct))
	return ct == "application/json" || strings.HasSuffix(ct, "+json")
}
