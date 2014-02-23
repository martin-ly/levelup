package levelup

import (
	"strings"
)

var (
	PrefixDelim = "^"
)

func checkPrefix(prefix string) (ok bool) {
	ok = strings.Index(prefix, PrefixDelim) <= 0
	return
}

func makeKey(prefix, key string) string {
	return prefix + PrefixDelim + key
}

func unMakeKey(key string) (string, string) {
	i := strings.Index(key, PrefixDelim)
	return key[0:i], key[i+1:]
}