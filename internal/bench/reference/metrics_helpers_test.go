package reference

import (
	"encoding/json"
	"testing"
)

func nativeEncoded(t *testing.T, native map[string]any) string {
	t.Helper()
	encoded, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
