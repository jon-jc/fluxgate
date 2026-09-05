package auth

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testSecret = "s3cr3t-value"

// testStore builds a store holding one usable key.
func testStore(t *testing.T, mutate func(*Key)) *StaticStore {
	t.Helper()

	key := Key{
		ID:           "k1",
		TenantID:     "acme",
		SecretSHA256: HashSecret(testSecret),
	}
	if mutate != nil {
		mutate(&key)
	}

	doc, err := json.Marshal([]Key{key})
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	store, err := ParseKeys(doc)
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	return store
}

func TestVerifyAcceptsAValidCredential(t *testing.T) {
	store := testStore(t, nil)

	principal, err := Verify(store, "fxg_k1_"+testSecret)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if principal.TenantID != "acme" {
		t.Errorf("TenantID = %q, want acme", principal.TenantID)
	}
	if principal.KeyID != "k1" {
		t.Errorf("KeyID = %q, want k1", principal.KeyID)
	}
}

func TestVerifyRejects(t *testing.T) {
	store := testStore(t, nil)

	tests := []struct {
		name  string
		token string
		want  error
	}{
		{name: "wrong secret", token: "fxg_k1_wrong", want: ErrBadSecret},
		{name: "unknown key", token: "fxg_nope_" + testSecret, want: ErrUnknownKey},
		{name: "no prefix", token: "k1_" + testSecret, want: ErrMalformedCredential},
		{name: "wrong prefix", token: "sk_k1_" + testSecret, want: ErrMalformedCredential},
		{name: "no secret", token: "fxg_k1", want: ErrMalformedCredential},
		{name: "empty secret", token: "fxg_k1_", want: ErrMalformedCredential},
		{name: "empty key id", token: "fxg__" + testSecret, want: ErrMalformedCredential},
		{name: "empty token", token: "", want: ErrMalformedCredential},
		{name: "just the prefix", token: "fxg", want: ErrMalformedCredential},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Verify(store, tc.token)
			if !errors.Is(err, tc.want) {
				t.Errorf("Verify(%q) = %v, want %v", tc.token, err, tc.want)
			}
		})
	}
}

// TestSecretIsMatchedInFull guards against a prefix comparison: a truncated
// secret must not authenticate.
func TestSecretIsMatchedInFull(t *testing.T) {
	store := testStore(t, nil)

	for _, token := range []string{
		"fxg_k1_" + testSecret[:len(testSecret)-1], // one character short
		"fxg_k1_" + testSecret + "x",               // one character long
		"fxg_k1_" + strings.ToUpper(testSecret),    // case must matter
	} {
		if _, err := Verify(store, token); err == nil {
			t.Errorf("Verify(%q) succeeded; the secret must match exactly", token)
		}
	}
}

func TestVerifyRejectsDisabledKey(t *testing.T) {
	store := testStore(t, func(k *Key) { k.Disabled = true })

	// The secret is correct; revocation is what must stop it.
	_, err := Verify(store, "fxg_k1_"+testSecret)
	if !errors.Is(err, ErrKeyDisabled) {
		t.Errorf("Verify = %v, want ErrKeyDisabled", err)
	}
}

// TestSecretIsNeverStoredInPlaintext is the property that makes a leaked
// configuration file survivable.
func TestSecretIsNeverStoredInPlaintext(t *testing.T) {
	store := testStore(t, nil)

	for _, key := range store.keys {
		if strings.Contains(key.SecretSHA256, testSecret) {
			t.Fatal("the plaintext secret appears in the stored record")
		}
		if string(key.digest) == testSecret {
			t.Fatal("the digest is the plaintext secret")
		}
	}
}

func TestVerifyCarriesPerKeyQuota(t *testing.T) {
	store := testStore(t, func(k *Key) {
		k.RateLimitPerSecond = 250
		k.Burst = 500
	})

	principal, err := Verify(store, "fxg_k1_"+testSecret)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if principal.RateLimitPerSecond != 250 || principal.Burst != 500 {
		t.Errorf("quota = %g/%d, want 250/500",
			principal.RateLimitPerSecond, principal.Burst)
	}
}

func TestParseKeysRejectsMalformedDocuments(t *testing.T) {
	valid := HashSecret("x")

	tests := []struct {
		name string
		doc  string
	}{
		{name: "not json", doc: `nonsense`},
		{name: "not an array", doc: `{"key_id":"k1"}`},
		{name: "empty array", doc: `[]`},
		{name: "missing key id", doc: `[{"tenant_id":"t","secret_sha256":"` + valid + `"}]`},
		{name: "missing tenant", doc: `[{"key_id":"k1","secret_sha256":"` + valid + `"}]`},
		{name: "underscore in key id", doc: `[{"key_id":"k_1","tenant_id":"t","secret_sha256":"` + valid + `"}]`},
		{name: "space in key id", doc: `[{"key_id":"k 1","tenant_id":"t","secret_sha256":"` + valid + `"}]`},
		// A tenant ID is the partition key for every stored row. A value that
		// only looks blank must fail the deployment rather than issue a
		// working credential scoped to a tenant nobody meant to create.
		{name: "whitespace-only tenant", doc: `[{"key_id":"k1","tenant_id":" ","secret_sha256":"` + valid + `"}]`},
		{name: "tab-only tenant", doc: `[{"key_id":"k1","tenant_id":"	","secret_sha256":"` + valid + `"}]`},
		{name: "tenant with trailing space", doc: `[{"key_id":"k1","tenant_id":"acme ","secret_sha256":"` + valid + `"}]`},
		{name: "tenant with leading space", doc: `[{"key_id":"k1","tenant_id":" acme","secret_sha256":"` + valid + `"}]`},
		{name: "whitespace-only key id", doc: `[{"key_id":" ","tenant_id":"t","secret_sha256":"` + valid + `"}]`},
		{name: "digest not hex", doc: `[{"key_id":"k1","tenant_id":"t","secret_sha256":"zzz"}]`},
		{name: "digest wrong length", doc: `[{"key_id":"k1","tenant_id":"t","secret_sha256":"abcd"}]`},
		{name: "negative rate", doc: `[{"key_id":"k1","tenant_id":"t","secret_sha256":"` + valid + `","rate_limit_per_second":-1}]`},
		{name: "unknown field", doc: `[{"key_id":"k1","tenant_id":"t","secret_sha256":"` + valid + `","admin":true}]`},
		{
			name: "duplicate key id",
			doc: `[{"key_id":"k1","tenant_id":"a","secret_sha256":"` + valid + `"},` +
				`{"key_id":"k1","tenant_id":"b","secret_sha256":"` + valid + `"}]`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A malformed credential document must fail the deployment, not
			// silently lock every caller out at runtime.
			if _, err := ParseKeys([]byte(tc.doc)); err == nil {
				t.Errorf("ParseKeys(%s) succeeded, want an error", tc.doc)
			}
		})
	}
}

func TestParseKeysAcceptsAFullDocument(t *testing.T) {
	doc := `[
	  {"key_id":"live1","tenant_id":"acme","secret_sha256":"` + HashSecret("a") + `",
	   "rate_limit_per_second":500,"burst":1000},
	  {"key_id":"live2","tenant_id":"globex","secret_sha256":"` + HashSecret("b") + `",
	   "disabled":true}
	]`

	store, err := ParseKeys([]byte(doc))
	if err != nil {
		t.Fatalf("ParseKeys: %v", err)
	}
	if store.Len() != 2 {
		t.Errorf("Len() = %d, want 2", store.Len())
	}
	if got := len(store.TenantIDs()); got != 2 {
		t.Errorf("TenantIDs() has %d entries, want 2", got)
	}
}

func TestHashSecretMatchesKnownDigest(t *testing.T) {
	// The digest of the empty string, so an operator can sanity-check the
	// hashing scheme against any standard tool.
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	if got := HashSecret(""); got != emptySHA256 {
		t.Errorf("HashSecret(\"\") = %q, want %q", got, emptySHA256)
	}
}

func TestLoadStore(t *testing.T) {
	doc := `[{"key_id":"k1","tenant_id":"acme","secret_sha256":"` + HashSecret(testSecret) + `"}]`

	t.Run("inline", func(t *testing.T) {
		store, err := LoadStore(doc, "")
		if err != nil {
			t.Fatalf("LoadStore: %v", err)
		}
		if store.Len() != 1 {
			t.Errorf("Len() = %d, want 1", store.Len())
		}
	})

	t.Run("from file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "keys.json")
		if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
			t.Fatalf("write key file: %v", err)
		}

		store, err := LoadStore("", path)
		if err != nil {
			t.Fatalf("LoadStore: %v", err)
		}
		if store.Len() != 1 {
			t.Errorf("Len() = %d, want 1", store.Len())
		}
	})

	t.Run("inline wins over file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "keys.json")
		if err := os.WriteFile(path, []byte(`[]`), 0o600); err != nil {
			t.Fatalf("write key file: %v", err)
		}

		// The file holds an empty document that would fail to parse; an
		// explicitly supplied inline value must take precedence.
		if _, err := LoadStore(doc, path); err != nil {
			t.Errorf("LoadStore: %v", err)
		}
	})

	t.Run("no source", func(t *testing.T) {
		if _, err := LoadStore("", ""); err == nil {
			t.Error("LoadStore with no source succeeded, want an error")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if _, err := LoadStore("", filepath.Join(t.TempDir(), "absent.json")); err == nil {
			t.Error("LoadStore with a missing file succeeded, want an error")
		}
	})
}
