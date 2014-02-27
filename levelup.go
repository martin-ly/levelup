package levelup

import (
	"github.com/jmhodges/levigo"
	"sync"
	"log"
	"path"
	"os"
)

const (
	DefaultBloomFilterSize = 10
)

// LevelUp is a wrapper around levigo DB objects with the following
// simplifications:
//   * useful, sane (at least to me) default options for db creation, 
//     read and write options.
//   * parameters for options we want to pass into the new function.
//   * Operations work on strings instead of []bytes. 
//   * concurrency-safe with a sync.RWMutex
//   * Remove() method returns the value removed.
//   * Move() method moves a value from one key to another.
//   * Required prefixes to enable simplier iteration.
//
// In short: We're sacrificing completeness for simplicity.
//
type LevelUp struct {
	db *levigo.DB
	l sync.RWMutex
	defRo *levigo.ReadOptions
	defWo *levigo.WriteOptions
}

func NewLevelUp(luPath string, fileSync bool, cacheSize int) (*LevelUp, error) {
	var l sync.RWMutex
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(cacheSize))
	opts.SetFilterPolicy(levigo.NewBloomFilter(DefaultBloomFilterSize))
	opts.SetCreateIfMissing(true)

	// create any subdirs if needed
	//
	luDir, _ := path.Split(luPath)
	if err := os.MkdirAll(luDir, os.ModeDir); err != nil {
		return nil, err
	}

	db, err := levigo.Open(luPath, opts)
	if err != nil {
		return nil, err
	}

	// default read / write options
	//
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	wo.SetSync(fileSync)

	lu := LevelUp{db, l, ro, wo}
	return &lu, nil
}


func (lu *LevelUp) Put(prefix, key, data string) {
	if !checkPrefix(prefix) {
		log.Panicf("prefix cannot have the prefix delim in it!")
	}
	lu.l.Lock()
	defer lu.l.Unlock()
	lu.put(makeKey(prefix, key), data)
}
func (lu *LevelUp) put(key, data string) {
	lu.db.Put(lu.defWo, []byte(key), []byte(data))
}


func (lu *LevelUp) Get(prefix, key string) string {
	lu.l.RLock()
	defer lu.l.RUnlock()
	return lu.get(makeKey(prefix, key))
}
func (lu *LevelUp) get(key string) string {
	data, err := lu.db.Get(lu.defRo, []byte(key))
	if err != nil || data == nil {
		return ""
	}
	return string(data)
}

func (lu *LevelUp) Remove(prefix, key string) string {
	lu.l.Lock()
	defer lu.l.Unlock()
	return lu.remove(makeKey(prefix, key))
}
func (lu *LevelUp) remove(key string) string {
	result := lu.get(key)
	err := lu.db.Delete(lu.defWo, []byte(key))
	if err != nil {
		log.Println("error removing key", key)
	}
	return result
}

func (lu *LevelUp) Move(fromPrefix, fromKey, toPrefix, toKey string) {
	lu.l.Lock()
	defer lu.l.Unlock()
	fromRealKey := makeKey(fromPrefix, fromKey)
	toRealKey := makeKey(toPrefix, toKey)
	data := lu.remove(fromRealKey)
	if data == "" {
		return
	}
	lu.put(toRealKey, data)
}

func (lu *LevelUp) Look(prefix, start string, limit int) []Visit {
	it := lu.getIterator()
	defer it.Close()
	result := make([]Visit, 0, limit)
	looker := func(prefix, key, value string) error {
		result = append(result, Visit{prefix, key, value})
		return nil
	}
	visitor := NewVisitor(prefix, it, looker)
	visitor.SetCursor(prefix, start)
	visitor.Visit(limit, true)
	return result
}

func (lu *LevelUp) Behind(prefix, start string) *Visit {
	it := lu.getIterator()
	defer it.Close()
	it.Seek([]byte(makeKey(prefix,start)))
	if !it.Valid() {
		it.SeekToLast()
		if !it.Valid() {
			return nil
		}
	} else {
		it.Prev()
		if !it.Valid() {
			return nil
		}
	}
	return lu.visitFromIterator(it)
}

func (lu *LevelUp) Last() *Visit {
	it := lu.getIterator()
	defer it.Close()
	if it.SeekToLast(); it.Valid() {
		return lu.visitFromIterator(it)
	} else {
		return nil
	}
}

func (lu *LevelUp) visitFromIterator(it *levigo.Iterator) *Visit {
	if !it.Valid() {
		return nil
	}
	prefix, key := unMakeKey(string(it.Key()))
	value := string(it.Value())
	return &Visit{prefix,key,value}
}


func (lu *LevelUp) getIterator() *levigo.Iterator {
	lu.l.RLock()
	defer lu.l.RUnlock()
	snap := lu.db.NewSnapshot()
	itRo := levigo.NewReadOptions()
	itRo.SetSnapshot(lu.db.NewSnapshot())
	defer lu.db.ReleaseSnapshot(snap)
	defer itRo.Close()
	return lu.db.NewIterator(itRo)
}

func (lu *LevelUp) Close() {
	lu.db.Close()
	lu.defRo.Close()
	lu.defWo.Close()
}



