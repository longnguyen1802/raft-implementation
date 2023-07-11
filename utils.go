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
	return time.Duration(600+rand.Intn(600)) * time.Millisecond
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if cm.getLogSize() > 0 {
		lastIndex := cm.getLogSize() - 1
		return lastIndex, cm.getTerm(lastIndex)
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

func (cm *ConsensusModule) getTermAndSliceForIndex(prevLogIndex int) (int,[]Log) {
	if prevLogIndex>=0 {
		return cm.log[prevLogIndex].Term,cm.log[prevLogIndex+1:]
	} else{
		return -1,cm.log
	}
	
	// lastIncludedIndex = cm.lastIncludedIndex
	// if index >= lastIncludedIndex {
	// 	return cm.log[index-lastIncludedIndex].Term,cm.log[index-lastIncludedIndex:]
	// }
}

func (cm *ConsensusModule) getTerm(index int) int {
	return cm.log[index].Term
}

func (cm *ConsensusModule) getLogSize() int {
	return len(cm.log)
}

func (cm *ConsensusModule) getLogSlice(from int,to int) []Log{
	return cm.log[from:to]
}