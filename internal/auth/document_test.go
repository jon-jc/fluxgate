package auth

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestKeyDocumentRejectsTrailingDataAndUnsafeIdentities(t *testing.T) {
	key := Key{ID: "safe", TenantID: "acme", SecretSHA256: HashSecret("secret")}
	good, err := json.Marshal([]Key{key})
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"[]", "{}", "null", "garbage"} {
		if _, err := ParseKeys(append(append([]byte(nil), good...), []byte(suffix)...)); err == nil {
			t.Fatalf("trailing %q accepted", suffix)
		}
	}
	for _, tenant := range []string{"a\x00b", "a\rb", strings.Repeat("a", 256)} {
		key.TenantID = tenant
		doc, err := json.Marshal([]Key{key})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseKeys(doc); err == nil {
			t.Fatalf("unsafe tenant %q accepted", tenant)
		}
	}
}
