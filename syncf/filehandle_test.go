package syncf

import (
	"log"
	"testing"
)

func TestFileStat(t *testing.T) {
	fileStat, err := GetFileStat("./util.go")
	log.Println(fileStat, err)
}
