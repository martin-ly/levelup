package levelup

import (
	"github.com/jmhodges/levigo"
	"errors"
)

var (
	errStop = errors.New("stop visiting")
)

type Visit struct {
	prefix string
	key    string
	value  string
}

type VisitFunc func(prefix, key, value string) error
type Visitor struct {
	it *levigo.Iterator
	fn  VisitFunc
	cursor []byte
}

func NewVisitor(prefix string, it *levigo.Iterator, fn VisitFunc) *Visitor {
	v := Visitor{
		it: it, 
		fn: fn, 
	}
	v.SetCursor(prefix, "")
	return &v
}

func (v *Visitor) SetCursor(prefix, key string) {
	start := makeKey(prefix, key)
	v.cursor = []byte(start)
}

func (v *Visitor) Visit(limit int) error {
	v.it.Seek(v.cursor)
	for ; v.it.Valid() && limit > 0; v.it.Next() {
		limit--
		prefix, key := unMakeKey(string(v.it.Key()))
		value := string(v.it.Value())
		if err := v.fn(prefix, key, value); err != nil {
			if err == errStop {
				return nil
			} else {
				return err
			}
		}
	}
	return v.it.GetError()
}