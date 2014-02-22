package levelup_test

import (
	"github.com/HVF/levelup"
	"testing"
	"encoding/hex"
	"crypto/rand"
	"os"
	"path"
	"log"
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

func TestLevelUp(t *testing.T) {
	testId := randString(4)
	t.Log("testId:", testId)
	lu, err := makeTestLevelUp(testId)
	defer lu.Close()
	if err != nil {
		t.Fatal(err)
	}
}