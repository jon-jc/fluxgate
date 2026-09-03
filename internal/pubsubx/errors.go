package pubsubx

import (
	"errors"
	"fmt"
)

// permanentError marks a failure that redelivery cannot fix.
//
// The distinction drives the single most consequential decision a consumer
// makes about a message. A transient failure -- the database is briefly
// unreachable -- must be retried, because the message is fine and the world
// will recover. A permanent failure -- the body is not valid JSON -- must not
// be, because the thousandth attempt will fail exactly like the first while
// consuming quota and blocking the subscription behind it.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return "permanent: " + p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks err as unfixable by retrying. A nil error stays nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked permanent anywhere in its chain.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// ErrOverloaded means the publisher's buffer is full.
//
// It is a deliberate rejection, not a fault: the alternative to shedding load
// is buffering without bound until the process is killed, which loses every
// message it was holding rather than the one it declined to accept.
var ErrOverloaded = errors.New("publisher is at capacity")

// ErrUnavailable means the publisher is failing fast because the dependency
// looks unhealthy.
var ErrUnavailable = errors.New("pub/sub is unavailable")

// wrapPublishError classifies a failure from the Pub/Sub client.
func wrapPublishError(topic string, err error) error {
	return fmt.Errorf("publish to %s: %w", topic, err)
}
