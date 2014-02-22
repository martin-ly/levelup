package levelup

import (
	"github.com/jmhodges/levigo"
	"sync"
	"strings"
	"log"
)

type LevelUp struct {
	db *levigo.DB
	l sync.RWMutex
	defRo *levigo.ReadOptions
	defWo *levigo.WriteOptions
}

func NewLevelUp(path string, fileSync bool) (*LevelUp, error) {
	var l sync.RWMutex
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1<<24))
	opts.SetFilterPolicy(levigo.NewBloomFilter(10))
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

	dq := LevelUp{db, l, ro, wo}
	return &dq, nil
}


func (dq *LevelUp) Put(prefix, key, data string) {
	dq.l.Lock()
	defer dq.l.Unlock()
	dq.put(makeKey(prefix, key), data)
}
func (dq *LevelUp) put(key, data string) {
	dq.db.Put(dq.defWo, []byte(key), []byte(data))
}


func (dq *LevelUp) Get(prefix, key string) string {
	dq.l.RLock()
	defer dq.l.RUnlock()
	return dq.get(makeKey(prefix, key))
}
func (dq *LevelUp) get(key string) string {
	data, err := dq.db.Get(dq.defRo, []byte(key))
	if err != nil || data == nil {
		return ""
	}
	return string(data)
}

func (dq *LevelUp) Remove(prefix, key string) string {
	dq.l.Lock()
	defer dq.l.Unlock()
	return dq.remove(makeKey(prefix, key))
}
func (dq *LevelUp) remove(key string) string {
	result := dq.get(key)
	err := dq.db.Delete(dq.defWo, []byte(key))
	if err != nil {
		log.Println("error removing key", key)
	}
	return result
}

func (dq *LevelUp) Move(fromPrefix, fromKey, toPrefix, toKey string) {
	dq.l.Lock()
	defer dq.l.Unlock()
	fromRealKey := makeKey(fromPrefix, fromKey)
	toRealKey := makeKey(toPrefix, toKey)
	data := dq.remove(fromRealKey)
	if data == "" {
		return
	}
	dq.put(toRealKey, data)
}

func (dq *LevelUp) Close() {
	dq.db.Close()
	dq.defRo.Close()
	dq.defWo.Close()
}



func makeKey(pfx, key string) string {
	return strings.Join([]string{pfx, key}, "/")
}

func unMakeKey(pfx, key string) string {
	return strings.TrimPrefix(key, pfx)
}

