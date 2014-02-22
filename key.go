package levelup

import (
	"strings"
)

var (
	PrefixDelim = string([]byte{0x00})
)

func checkPrefix(prefix string) bool {
	return strings.Index(prefix, PrefixDelim) > 0
}

func makeKey(prefix, key string) string {
	return prefix + PrefixDelim + key
}

func unMakeKey(key string) (string, string) {
	i := strings.Index(key, PrefixDelim)
	return key[0:i], key[i+1:]
}