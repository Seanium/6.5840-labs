package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = false

var debugStart time.Time

func init() {
	debugStart = time.Now()
	// remove date and time
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d - ", time)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
