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
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return getIndexFromLogEntry(lastIndex,cm.lastIncludedIndex), cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
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

// Need to lock the file before use it or 
func getLogEntryIndex(index int,lastIncludedIndex int) int {
	if lastIncludedIndex <= 2*SNAPSHOT_LOGSIZE {
		return index
	} else {
		return index - lastIncludedIndex + 2*SNAPSHOT_LOGSIZE
	}
}

func getIndexFromLogEntry(index int,lastIncludedIndex int) int{
	if lastIncludedIndex <= 2*SNAPSHOT_LOGSIZE {
		return index
	} else {
		return index + lastIncludedIndex - 2*SNAPSHOT_LOGSIZE
	}
}