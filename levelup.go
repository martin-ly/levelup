package levelup

import (
	"github.com/jmhodges/levigo"
	"sync"
	"strings"
	"log"
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

func NewLevelUp(path string, fileSync bool, cacheSize int) (*LevelUp, error) {
	var l sync.RWMutex
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(cacheSize))
	opts.SetFilterPolicy(levigo.NewBloomFilter(DefaultBloomFilterSize))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
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

func (lu *LevelUp) Close() {
	lu.db.Close()
	lu.defRo.Close()
	lu.defWo.Close()
}

func makeKey(pfx, key string) string {
	return strings.Join([]string{pfx, key}, "/")
}

func unMakeKey(pfx, key string) string {
	return strings.TrimPrefix(key, pfx)
}

