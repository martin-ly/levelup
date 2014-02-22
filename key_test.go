package levelup

import (
	"testing"
)

func TestKeyMakings(t *testing.T) {
	samples := []struct{
		p, k, check string
	}{
		{"people", "joe", "people" + string([]byte{0x00}) + "joe"},
	}
	for _, s := range samples {
		realKey := makeKey(s.p, s.k)
		if realKey != s.check {
			t.Fatal("mismatch", s, realKey)
		}
		prefix, key := unMakeKey(realKey)
		if prefix != s.p || key != s.k {
			t.Fatal("mismatch", []byte(prefix), []byte(s.p), []byte(key), []byte(s.k))
		}
	}


}