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
	dqPath := path.Join("/tmp", "testLevelUp", testId)
	if err := os.MkdirAll(dqPath, os.ModeDir); err != nil {
		return nil, err
	}
	return levelup.NewLevelUp(dqPath, false)
}

func TestLevelUp(t *testing.T) {
	testId := randString(4)
	t.Log("testId:", testId)
	dq, err := makeTestLevelUp(testId)
	defer dq.Close()
	if err != nil {
		t.Fatal(err)
	}
}