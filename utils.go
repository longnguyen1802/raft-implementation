package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var loglock sync.Mutex

/***********************************  Utility function *****************************************************/
// According to the paper setting timeout to 300ms - 600ms

func (cm *ConsensusModule) timeoutDuration() time.Duration {
	return time.Duration(300+rand.Intn(600)) * time.Millisecond
}

func (cm *ConsensusModule) debugLog(format string, args ...interface{}) {
	f, err := os.OpenFile("debuglog/server"+strconv.Itoa(cm.id)+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	loglock.Lock()
	log.SetOutput(f)
	format = fmt.Sprintf("[%d] ", cm.id) + format
	log.Printf(format, args...)
	defer loglock.Unlock()
}
