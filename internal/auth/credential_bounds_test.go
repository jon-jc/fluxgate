package auth

import (
	"strings"
	"testing"
)

func TestUntrustedCredentialPartsAreBoundedBeforeLogging(t *testing.T) {
	for _, token := range []string{
		"fxg_" + strings.Repeat("a", 129) + "_secret",
		"fxg_key_secret\tvalue", "fxg_bad/key_secret",
		"fxg_key_" + strings.Repeat("a", 1025),
	} {
		if _, _, err := splitToken(token); err == nil {
			t.Fatal("unbounded credential accepted")
		}
		if keyIDFor(token) != "" {
			t.Fatal("malformed credential reached log fields")
		}
	}
}
