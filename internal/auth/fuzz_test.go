package auth

import (
	"errors"
	"strings"
	"testing"
)

// FuzzVerify asserts that no token string can authenticate against a store it
// does not hold a secret for.
//
// Verify is the entire authentication boundary: whatever it returns a Principal
// for is treated by the rest of the service as an authenticated tenant, with
// that tenant's data and quota. A token is fully attacker-controlled, so the
// only acceptable outcome for anything but the real credential is an error --
// and the error must never leak which half of the guess was right.
func FuzzVerify(f *testing.F) {
	f.Add("fxg_key1_s3cret")
	f.Add("")
	f.Add("_")
	f.Add("fxg_key1_")
	f.Add("fxg__s3cret")
	f.Add("fxg_key1_s3cret_extra")
	f.Add("fxg_key1__s3cret")
	f.Add("FXG_KEY1_S3CRET")
	f.Add("fxg_key1_s3cre")
	f.Add("fxg_key1_s3crett")
	f.Add("fxg_key2_another")
	f.Add(strings.Repeat("a", 4096) + "_" + strings.Repeat("b", 4096))

	const (
		realKey    = "key1"
		realSecret = "s3cret"
	)
	store, err := ParseKeys([]byte(`[
		{"key_id":"key1","tenant_id":"acme","secret_sha256":"` +
		HashSecret(realSecret) + `"},
		{"key_id":"key2","tenant_id":"other","secret_sha256":"` +
		HashSecret("another") + `","disabled":true}
	]`))
	if err != nil {
		f.Fatalf("seed store: %v", err)
	}

	f.Fuzz(func(t *testing.T, token string) {
		principal, err := Verify(store, token)

		if err == nil {
			// The only token in existence that may succeed.
			if token != KeyPrefix+"_"+realKey+"_"+realSecret {
				t.Fatalf("token %q authenticated as tenant %q without being "+
					"the issued credential", token, principal.TenantID)
			}
			if principal.TenantID != "acme" || principal.KeyID != realKey {
				t.Fatalf("valid credential produced the wrong identity: %+v",
					principal)
			}
			return
		}

		// A rejection must hand back nothing usable. A partially populated
		// Principal is how a downstream caller that checks only the error of a
		// two-value return ends up acting on an attacker's tenant.
		if principal != (Principal{}) {
			t.Fatalf("rejected token %q still returned identity %+v",
				token, principal)
		}

		// The error must not distinguish a wrong secret from an unknown key in
		// a way that lets an attacker enumerate valid key IDs offline. Both are
		// sentinel values the middleware maps to one opaque 401; assert they
		// stay among the known set rather than growing a descriptive variant.
		if !errors.Is(err, ErrUnknownKey) &&
			!errors.Is(err, ErrBadSecret) &&
			!errors.Is(err, ErrKeyDisabled) &&
			!errors.Is(err, ErrMalformedCredential) {
			t.Fatalf("unexpected error variety for %q: %v", token, err)
		}
	})
}

// FuzzParseKeys asserts that a malformed key document is rejected rather than
// producing a store that authenticates something unintended.
//
// The document comes from an environment variable or a mounted secret. A
// deployment typo must fail loudly at startup; the outcome that matters is that
// it never yields a usable store with a key nobody meant to issue -- in
// particular one with an empty ID, an empty tenant, or an empty digest, any of
// which would turn a blank credential into a valid one.
func FuzzParseKeys(f *testing.F) {
	f.Add(`[{"key_id":"k","tenant_id":"t","secret_sha256":"` +
		HashSecret("s") + `"}]`)
	f.Add(`[]`)
	f.Add(`[{}]`)
	f.Add(`[{"key_id":"","tenant_id":"","secret_sha256":""}]`)
	f.Add(`[{"key_id":"k","tenant_id":"t","secret_sha256":"nothex"}]`)
	f.Add(`[{"key_id":"k","tenant_id":"t"},{"key_id":"k","tenant_id":"u"}]`)
	f.Add(`{"keys":[]}`)
	f.Add(`null`)
	f.Add(``)

	f.Fuzz(func(t *testing.T, doc string) {
		store, err := ParseKeys([]byte(doc))
		if err != nil {
			if store != nil {
				t.Fatalf("ParseKeys returned both a store and an error, so a "+
					"caller that logs and continues would run on it: %v", err)
			}
			return
		}
		if store == nil {
			t.Fatal("ParseKeys reported success with no store")
		}

		// An accepted document must not have produced a credential that an
		// empty or trivially guessed token would satisfy.
		for _, tenant := range store.TenantIDs() {
			if strings.TrimSpace(tenant) == "" {
				t.Fatalf("accepted a key with a blank tenant from %q", doc)
			}
		}
		for _, token := range []string{"", "_", "__", "fxg_a_b", "fxg__b", "fxg_a_", "a_b_c"} {
			if p, err := Verify(store, token); err == nil {
				t.Fatalf("document %q produced a store that accepts %q as "+
					"tenant %q", doc, token, p.TenantID)
			}
		}
	})
}
