package raft

import (
	"fmt"
)

// Want to make the system more robust and easy to change, might have many way to save the log of the node. But need to follwo the rule
// Log util method is directly use in the consesus algorithm for RequestVote and AppendEntries should not change the rule (can change the implementation)

/****************************************** Log utils function ***************************************************/
// Rule: Should return the term of a index and the slice of log from next index up to end of the log
func (cm *ConsensusModule) getTermAndSliceForIndex(prevLogIndex int) (int, []Log) {
	if prevLogIndex >= 0 {
		return cm.getTerm(prevLogIndex), cm.getLogSlice(prevLogIndex+1, cm.getLogSize())
	} else {
		return -1, cm.log
	}
}

// Rule: Return a term of scecific index
func (cm *ConsensusModule) getTerm(index int) int {
	//return cm.log[index].Term
	if index >= cm.lastIncludedIndex {
		return cm.log[index-cm.lastIncludedIndex].Term
	}
	filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, index/SNAPSHOT_LOGSIZE+1)
	snapshot, err := GetSnapshot(filename)
	if err != nil {
		cm.debugLog("Error when get snapshot from file %v", filename)
	}
	return snapshot.Logs[index%SNAPSHOT_LOGSIZE].Term
}

// Rule: Returm the actual size of log (Both store in stable or RAM)
func (cm *ConsensusModule) getLogSize() int {
	//return len(cm.log)
	return cm.lastIncludedIndex + len(cm.log)
}

// Return start of the index that in the log (in RAM)
func (cm *ConsensusModule) getLogStartIndex() int {
	return cm.lastIncludedIndex
}

// Rule: return a slice (might query from stable storage, need to balance this)
func (cm *ConsensusModule) getLogSlice(from int, to int) []Log {
	//return cm.log[from:to]

	if from >= cm.lastIncludedIndex {
		return cm.log[from-cm.lastIncludedIndex : to-cm.lastIncludedIndex]
	}
	begin := from
	logSlice := make([]Log, to-from)
	if to > cm.lastIncludedIndex {
		copy(logSlice[len(logSlice)-(to-cm.lastIncludedIndex):], cm.log)
		to = cm.lastIncludedIndex
	}
	for {
		filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, from/SNAPSHOT_LOGSIZE+1)
		snapshot, err := GetSnapshot(filename)
		if err != nil {
			cm.debugLog("Error when get snapshot from file %v", filename)
		}
		if from/SNAPSHOT_LOGSIZE == to/SNAPSHOT_LOGSIZE {
			copy(logSlice[from-begin:to-begin], snapshot.Logs[from%SNAPSHOT_LOGSIZE:to%SNAPSHOT_LOGSIZE])
			break
		} else {
			copy(logSlice[from-begin:(from/SNAPSHOT_LOGSIZE+1)*SNAPSHOT_LOGSIZE-begin], snapshot.Logs[from%SNAPSHOT_LOGSIZE:])
			from = (from/SNAPSHOT_LOGSIZE + 1) * SNAPSHOT_LOGSIZE
			if from == to {
				break
			}
		}
	}
	return logSlice
}

// For Request vote
// Return the lastIndex and last term
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if cm.getLogSize() > 0 {
		lastIndex := cm.getLogSize() - 1
		return lastIndex, cm.getTerm(lastIndex)
	} else {
		return -1, -1
	}
}

/********************************** State Getter and Setter Function ******************************/
func (cm *ConsensusModule) getLastIncludedIndex() int {
	return cm.lastIncludedIndex
}

func (cm *ConsensusModule) setLastIncludedIndex(newIncludedIndex int) {
	cm.lastIncludedIndex = newIncludedIndex
}

func (cm *ConsensusModule) getLastIncludedTerm() int {
	return cm.lastIncludedTerm
}

func (cm *ConsensusModule) setLastIncludedTerm(newIncludedTerm int) {
	cm.lastIncludedTerm = newIncludedTerm
}

func (cm *ConsensusModule) getCurrentTerm() int {
	return cm.currentTerm
}

func (cm *ConsensusModule) setCurrentTerm(newTerm int) {
	cm.currentTerm = newTerm
}

func (cm *ConsensusModule) getVotedFor() int {
	return cm.votedFor
}

func (cm *ConsensusModule) setVotedFor(peerId int) {
	cm.votedFor = peerId
}

func (cm *ConsensusModule) getLog() []Log {
	return cm.log
}

func (cm *ConsensusModule) setLog(newLog []Log) {
	cm.log = newLog
}

func (cm *ConsensusModule) extendLog(newLog []Log) {
	cm.log = append(cm.log, newLog...)
}

func (cm *ConsensusModule) appendLog(elem Log) {
	cm.log = append(cm.log, elem)
}

func (cm *ConsensusModule) getCommitIndex() int {
	return cm.commitIndex
}

func (cm *ConsensusModule) setCommitIndex(newCommitIndex int) {
	cm.commitIndex = newCommitIndex
}

func (cm *ConsensusModule) getAllNextIndex() map[int]int {
	return cm.nextIndex
}

func (cm *ConsensusModule) getNextIndex(peerId int) int {
	return cm.nextIndex[peerId]
}

func (cm *ConsensusModule) setNextIndex(peerId int, newNextIndex int) {
	cm.nextIndex[peerId] = newNextIndex
}

func (cm *ConsensusModule) getAllMatchIndex() map[int]int {
	return cm.matchIndex
}

func (cm *ConsensusModule) getMatchndex(peerId int) int {
	return cm.matchIndex[peerId]
}

func (cm *ConsensusModule) setMatchIndex(peerId int, newMatchIndex int) {
	cm.matchIndex[peerId] = newMatchIndex
}
func (cm *ConsensusModule) getAllMatchIncludedIndex() map[int]int {
	return cm.matchIncludedIndex
}

func (cm *ConsensusModule) getMatchIncludedIndex(peerId int) int {
	return cm.matchIncludedIndex[peerId]
}

func (cm *ConsensusModule) setMatchIncludedIndex(peerId int, newMatchIncludedIndex int) {
	cm.matchIncludedIndex[peerId] = newMatchIncludedIndex
}
