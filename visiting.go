package levelup

import (
	"github.com/jmhodges/levigo"
	"errors"
)

var (
	FirstDelim = string([]byte{0x00, 0x00, 0x00, 0x00})
	LastDelim  = string([]byte{0xFF, 0xFF, 0xFF, 0xFF})
)
var (
	errStop = errors.New("stop visiting")
)

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
	start := makeKey(prefix, FirstDelim)
	return &Visitor{
		it: it, 
		nx: NextFunc, 
		fn: fn, 
		cursor: []byte(start),
	}
}

func NewBackwardVisitor(prefix string, it *levigo.Iterator, fn VisitFunc) *Visitor {
	start := makeKey(prefix, LastDelim)
	return &Visitor{
		it: it, 
		nx: PrevFunc, 
		fn: fn, 
		cursor: []byte(start),
	}
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








// func (dq *LevelUp) Push(prefix, key, data string) {
// 	dq.l.Lock()
// 	defer dq.l.Unlock()
// 	dq.put(makeKey(prefix, key), data)
// }

// func (dq *LevelUp) Peek(prefix string, limit int) []Pair {
// 	dq.l.RLock()
// 	defer dq.l.RUnlock()
// 	result := make([]Pair, 0, limit)
// 	peeker := func(p *Pair) {
// 		result = append(result, *p)
// 	}
// 	dq.visit(prefix, peeker, limit)

// 	return result
// }

// func (dq *LevelUp) Pop(prefix string, limit int) []Pair {
// 	dq.l.Lock()
// 	defer dq.l.Unlock()
// 	result := make([]Pair, 0, limit)
// 	popper := func(p *Pair) {
// 		result = append(result, *p)
		
// 	}
// 	dq.visit(prefix, popper, limit)

// 	return result
// }



