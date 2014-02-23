package levelup_test

import (
	"github.com/HVF/levelup"
	"testing"
	"encoding/hex"
	"crypto/rand"
	"os"
	"path"
	"log"
	"fmt"
	"sort"
)

func randString(c int) string {
	b := make([]byte, c)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal("creating random test id")
	}
	s := hex.EncodeToString(b)
	return s
}

func makeTestLevelUp(testId string) (*levelup.LevelUp, error) {
	luPath := path.Join("/tmp", "testLevelUp", testId)
	if err := os.MkdirAll(luPath, os.ModeDir); err != nil {
		return nil, err
	}
	return levelup.NewLevelUp(luPath, false, 1<<10)
}

func TestLevelUpBasic(t *testing.T) {
	testId := randString(4)
	t.Log("testId:", testId)
	lu, err := makeTestLevelUp(testId)
	if err != nil {
		t.Fatal(err)
	}
	defer lu.Close()
	// put & get
	lu.Put("test", "foo", "bar")
	if r := lu.Get("test", "foo"); r != "bar" {
		t.Fatal("mistmatch", r)
	}
	// remove
	if r1 := lu.Remove("test", "foo"); r1 != "bar" {
		t.Fatal("mistmatch", r1)
	} else {
		if r2 := lu.Get("test", "foo"); r2 != "" {
			t.Fatal("test foo still there", r2)
		}
	}
	// move
	lu.Put("more/test", "fun", "facts")
	lu.Put("less/test", "lame", "facts")
	lu.Move("more/test", "fun", "less/test", "fun")
	if r := lu.Get("less/test", "fun"); r != "facts" {
		t.Fatal("mismatch", r)
	}
	if r := lu.Get("more/test", "fun"); r != "" {
		t.Fatal("still there", r)
	}

}

type TestRow struct {
	prefix, key, value string
}

func (tr TestRow) String() string {
	return fmt.Sprintf("%s%s = %s", tr.prefix, tr.key, tr.value)
}

type TestData []TestRow

func (d TestData) Len() int {
	return len(d)
}
func (d TestData) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}
func (d TestData) Less(i, j int) bool {
	return (d[i].prefix + d[i].key) < (d[j].prefix + d[j].key)
}


// some test data, with rows keyed by 
var testDataByPrefix = map[string]TestData{
	"person": TestData{
		TestRow{"person", "joe", "my name is joe and i'm a person"},
		TestRow{"person", "mark", "person by the name of mark"},
		TestRow{"person", "Mark", "important Mr Mark"},
	}, 
	"animal": TestData{
		TestRow{"animal", "tiger", "tiger tiger burning bright"},
		TestRow{"animal", "bear", "bear. Bear! BEAR!!!"},		
	}, 
	"spaceship": TestData{
		TestRow{"spaceship", "Enterprise", "these are the voyages"},
	},
}

func sortData(td map[string]TestData) {
	for _, rows := range td {
		sort.Sort(rows)
	}
}

func checkVisit(v levelup.Visit, tr TestRow) bool {
	return levelup.VisitString(&v) == tr.String()
}

func TestVisiting(t *testing.T) {
	testId := randString(4)
	t.Log("testId:", testId)
	lu, err := makeTestLevelUp(testId)
	if err != nil {
		t.Fatal(err)
	}
	defer lu.Close()

	// add the test data to the store.
	//
	for _, td := range testDataByPrefix {
		for _, tr := range td {
			lu.Put(tr.prefix, tr.key, tr.value)
		}
	}
	// after it's add, sort each prefix-list so we can check against it.
	//
	sortData(testDataByPrefix)

	// check first rows
	//
	for prefix, data := range testDataByPrefix {
		// forward
		//
		result := lu.LookForward(prefix, "", 1)
		if len(result) != 1 {
			t.Fatal("too long")
		} 
		if !checkVisit(result[0], data[0]) {
			t.Fatal("mismatch", result, data[0])
		}
		
		// backward
		//
		result = lu.LookBackward(prefix, "", 1)
		if len(result) != 1 {
			t.Fatal("too long", result)
		} 
		if !checkVisit(result[0], data[len(data)-1]) {
			t.Fatal("mismatch", result, data[len(data)-1])
		}

	}

	

}