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

/****************************************** Log utils function ***************************************************/
func (cm *ConsensusModule) getTermAndSliceForIndex(prevLogIndex int) (int,[]Log) {
	if prevLogIndex>=0 {
		return cm.getTerm(prevLogIndex),cm.getLogSlice(prevLogIndex+1,cm.getLogSize())
	} else{
		return -1,cm.log
	}
}

func (cm *ConsensusModule) getTerm(index int) int {
	//return cm.log[index].Term
	if index >= cm.lastIncludedIndex {
		return cm.log[index-cm.lastIncludedIndex].Term
	}
	filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, index/SNAPSHOT_LOGSIZE+1)
	snapshot,err := GetSnapshot(filename)
	if err !=nil {
		cm.debugLog("Error when get snapshot from file %v",filename)
	}
	return snapshot.Logs[index%SNAPSHOT_LOGSIZE].Term
}

func (cm *ConsensusModule) getLogSize() int {
	//return len(cm.log)
	return cm.lastIncludedIndex + len(cm.log)
}

func (cm *ConsensusModule) getLogSlice(from int,to int) []Log{
	//return cm.log[from:to]
	
	if from >= cm.lastIncludedIndex {
		return cm.log[from-cm.lastIncludedIndex:to-cm.lastIncludedIndex]
	}
	begin := from
	logSlice := make([]Log,to-from)
	if to > cm.lastIncludedIndex {
		copy(logSlice[len(logSlice)-(to-cm.lastIncludedIndex):],cm.log)
		to=cm.lastIncludedIndex
	}
	for {
		filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, from/SNAPSHOT_LOGSIZE+1)
		snapshot,err := GetSnapshot(filename)
		if err !=nil {
			cm.debugLog("Error when get snapshot from file %v",filename)
		}
		if from/SNAPSHOT_LOGSIZE == to/SNAPSHOT_LOGSIZE {
			copy(logSlice[from-begin:to-begin],snapshot.Logs[from%SNAPSHOT_LOGSIZE:to%SNAPSHOT_LOGSIZE])
			break
		} else{
			copy(logSlice[from-begin:(from/SNAPSHOT_LOGSIZE+1)*SNAPSHOT_LOGSIZE-begin],snapshot.Logs[from%SNAPSHOT_LOGSIZE:])
			from = (from/SNAPSHOT_LOGSIZE+1)*SNAPSHOT_LOGSIZE
			if from == to {
				break
			}
		}
	}
	return logSlice
}
