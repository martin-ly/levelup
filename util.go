package levelup

import (
	"fmt"
	"log"
)

func VisitString(v *Visit) string {
	return fmt.Sprintf("%s%s = %s", v.prefix, v.key, v.value)
}

func PrintRawLevelUp(lu *LevelUp) {
	it := lu.getIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		log.Printf("%-25s %v %v", string(it.Key()), it.Key(), it.Value())
	}
}