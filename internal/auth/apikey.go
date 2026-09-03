// Package auth authenticates API callers and resolves them to a tenant.
//
// Fluxgate uses bearer API keys rather than sessions: callers are machines,
// keys are long-lived, and each one is scoped to exactly one tenant. The store
// never holds a plaintext secret -- only a SHA-256 digest -- so a leak of the
// configuration does not hand an attacker working credentials.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// KeyPrefix marks a Fluxgate credential. A distinctive prefix lets secret
// scanners recognise a leaked key in a commit or a log and revoke it before
// anyone else finds it.
const KeyPrefix = "fxg"

// Key is a credential record. The plaintext secret is never stored.
type Key struct {
	// ID identifies the key and is safe to log.
	ID string `json:"key_id"`
	// TenantID is the tenant every request with this key belongs to.
	TenantID string `json:"tenant_id"`
	// SecretSHA256 is the hex-encoded digest of the secret half of the key.
	SecretSHA256 string `json:"secret_sha256"`
	// Disabled revokes the key without removing it, which preserves the audit
	// trail of what the key was and who it belonged to.
	Disabled bool `json:"disabled,omitempty"`
	// RateLimitPerSecond is the sustained request rate allowed for this key.
	// Zero means the service default applies.
	RateLimitPerSecond float64 `json:"rate_limit_per_second,omitempty"`
	// Burst is how many requests may arrive at once before throttling starts.
	// Zero means the service default applies.
	Burst int `json:"burst,omitempty"`

	// digest is the decoded SecretSHA256, resolved once at load time so that
	// verification does no hex decoding on the request path.
	digest []byte
}

// Principal is the authenticated caller, attached to the request context.
type Principal struct {
	// TenantID owns the data this request carries.
	TenantID string
	// KeyID identifies which credential was used, for audit and revocation.
	KeyID string
	// RateLimitPerSecond and Burst carry the key's quota, zero when the
	// service default applies.
	RateLimitPerSecond float64
	Burst              int
}

// Store resolves a key ID to its record.
type Store interface {
	// Lookup returns the key with the given ID. The boolean reports whether it
	// exists; implementations must not distinguish "missing" from "disabled"
	// in their timing.
	Lookup(keyID string) (Key, bool)
}

// StaticStore is an immutable in-memory key set loaded at startup.
//
// Keys change rarely and the set is small, so an immutable map read without a
// lock is both the simplest and the fastest option. Rotating a key means
// redeploying with new configuration, which is deliberate: it leaves an audit
// trail in the deployment history.
type StaticStore struct {
	keys map[string]Key
}

// Lookup implements Store.
func (s *StaticStore) Lookup(keyID string) (Key, bool) {
	k, ok := s.keys[keyID]
	return k, ok
}

// Len reports how many keys are loaded, for startup logging.
func (s *StaticStore) Len() int { return len(s.keys) }

// TenantIDs returns the distinct tenants represented, for startup logging.
func (s *StaticStore) TenantIDs() []string {
	seen := make(map[string]struct{}, len(s.keys))
	for _, k := range s.keys {
		seen[k.TenantID] = struct{}{}
	}
	tenants := make([]string, 0, len(seen))
	for t := range seen {
		tenants = append(tenants, t)
	}
	return tenants
}

// ParseKeys builds a StaticStore from a JSON array of key records.
//
// Every record is validated up front so a malformed credential file fails the
// deployment rather than silently locking every caller out at runtime.
func ParseKeys(doc []byte) (*StaticStore, error) {
	var records []Key
	dec := json.NewDecoder(strings.NewReader(string(doc)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&records); err != nil {
		return nil, fmt.Errorf("parse API key document: %w", err)
	}

	keys := make(map[string]Key, len(records))
	for i, k := range records {
		switch {
		case k.ID == "":
			return nil, fmt.Errorf("key %d: key_id is required", i)
		case k.TenantID == "":
			return nil, fmt.Errorf("key %q: tenant_id is required", k.ID)
		case strings.ContainsAny(k.ID, "_ \t\n"):
			// The credential format joins its parts with underscores, so an
			// underscore in the ID would make the split ambiguous.
			return nil, fmt.Errorf("key %q: key_id must not contain whitespace or '_'", k.ID)
		}

		digest, err := hex.DecodeString(k.SecretSHA256)
		if err != nil {
			return nil, fmt.Errorf("key %q: secret_sha256 is not valid hex: %w", k.ID, err)
		}
		if len(digest) != sha256.Size {
			return nil, fmt.Errorf(
				"key %q: secret_sha256 must be a %d-byte SHA-256 digest, got %d bytes",
				k.ID, sha256.Size, len(digest))
		}
		if k.RateLimitPerSecond < 0 {
			return nil, fmt.Errorf("key %q: rate_limit_per_second must not be negative", k.ID)
		}
		if k.Burst < 0 {
			return nil, fmt.Errorf("key %q: burst must not be negative", k.ID)
		}

		if _, duplicate := keys[k.ID]; duplicate {
			return nil, fmt.Errorf("key %q: duplicate key_id", k.ID)
		}

		k.digest = digest
		keys[k.ID] = k
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("API key document contains no keys")
	}
	return &StaticStore{keys: keys}, nil
}

// HashSecret returns the hex-encoded SHA-256 digest of a plaintext secret, in
// the form the key document expects.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Credential errors. Callers map all of them onto an identical 401 so that a
// probing client learns nothing about which part was wrong.
var (
	// ErrMalformedCredential means the token did not have the expected shape.
	ErrMalformedCredential = fmt.Errorf("malformed credential")
	// ErrUnknownKey means no key with that ID exists.
	ErrUnknownKey = fmt.Errorf("unknown key")
	// ErrBadSecret means the secret did not match the stored digest.
	ErrBadSecret = fmt.Errorf("secret mismatch")
	// ErrKeyDisabled means the key exists but has been revoked.
	ErrKeyDisabled = fmt.Errorf("key disabled")
)

// dummyDigest stands in for a missing key so that verification performs the
// same work whether or not the key ID exists. Without it, an unknown key would
// return measurably faster than a known one with a wrong secret, letting an
// attacker enumerate valid key IDs by timing alone.
var dummyDigest = func() []byte {
	sum := sha256.Sum256([]byte("fluxgate/timing-equalisation-placeholder"))
	return sum[:]
}()

// Verify authenticates a bearer token and resolves it to a Principal.
//
// The token format is "fxg_<key id>_<secret>". Splitting the identifier from
// the secret means the server can look up exactly one candidate record instead
// of comparing the presented secret against every key it knows.
func Verify(store Store, token string) (Principal, error) {
	keyID, secret, err := splitToken(token)
	if err != nil {
		// Still run a comparison so a malformed token costs the same as a
		// well-formed one with a bad secret.
		compare(dummyDigest, "")
		return Principal{}, err
	}

	key, found := store.Lookup(keyID)
	digest := key.digest
	if !found {
		digest = dummyDigest
	}

	matches := compare(digest, secret)

	switch {
	case !found:
		return Principal{}, ErrUnknownKey
	case !matches:
		return Principal{}, ErrBadSecret
	case key.Disabled:
		return Principal{}, ErrKeyDisabled
	}

	return Principal{
		TenantID:           key.TenantID,
		KeyID:              key.ID,
		RateLimitPerSecond: key.RateLimitPerSecond,
		Burst:              key.Burst,
	}, nil
}

// compare hashes the presented secret and compares it in constant time. A
// plain == on the digests would leak, through its early exit, how many leading
// bytes were correct.
func compare(want []byte, secret string) bool {
	got := sha256.Sum256([]byte(secret))
	return subtle.ConstantTimeCompare(want, got[:]) == 1
}

// splitToken parses "fxg_<key id>_<secret>".
func splitToken(token string) (keyID, secret string, err error) {
	prefix, rest, ok := strings.Cut(token, "_")
	if !ok || prefix != KeyPrefix {
		return "", "", ErrMalformedCredential
	}
	keyID, secret, ok = strings.Cut(rest, "_")
	if !ok || keyID == "" || secret == "" {
		return "", "", ErrMalformedCredential
	}
	return keyID, secret, nil
}

// LoadStore builds a key store from an inline JSON document or a file.
//
// The inline form suits local development and CI; the file form is how a
// secret manager delivers credentials into a container, mounted as a volume so
// that rotating them does not require rebuilding an image. Inline wins when
// both are set, because an explicitly supplied value should never be silently
// overridden by an ambient one.
func LoadStore(inline, path string) (*StaticStore, error) {
	if inline != "" {
		return ParseKeys([]byte(inline))
	}
	if path == "" {
		return nil, fmt.Errorf("no API key source configured")
	}

	// The path comes from process configuration set by whoever deploys the
	// service, not from any request, so it is trusted by construction.
	doc, err := os.ReadFile(path) //nolint:gosec // operator-supplied config path
	if err != nil {
		return nil, fmt.Errorf("read API key file %q: %w", path, err)
	}
	return ParseKeys(doc)
}
