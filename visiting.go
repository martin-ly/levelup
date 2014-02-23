package levelup

import (
	"github.com/jmhodges/levigo"
	"errors"
)

var (
	FirstBookend = string([]byte{})
	LastBookend  = string([]byte{0xFF, 0xFF, 0xFF, 0xFF})
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
type IterFunc func(it *levigo.Iterator)
var (
	NextFunc = func(it *levigo.Iterator) { it.Next() }
	PrevFunc = func(it *levigo.Iterator) { it.Prev() }
)
type Visitor struct {
	it *levigo.Iterator
	nx  IterFunc
	fn  VisitFunc
	cursor []byte
}

func NewForwardVisitor(prefix string, it *levigo.Iterator, fn VisitFunc) *Visitor {
	v := Visitor{
		it: it, 
		nx: NextFunc, 
		fn: fn, 
	}
	v.SetCursor(prefix, FirstBookend)
	return &v
}

func NewBackwardVisitor(prefix string, it *levigo.Iterator, fn VisitFunc) *Visitor {
	v := Visitor{
		it: it, 
		nx: PrevFunc, 
		fn: fn, 
	}
	v.SetCursor(prefix, LastBookend)
	return &v
}

func (v *Visitor) SetCursor(prefix, key string) {
	start := makeKey(prefix, key)
	v.cursor = []byte(start)
}

func (v *Visitor) Visit(limit int) error {
	v.it.Seek(v.cursor)
	for ; v.it.Valid() && limit > 0; v.nx(v.it) {
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