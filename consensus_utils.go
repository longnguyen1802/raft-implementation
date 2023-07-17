package raft

import (
	"github.com/nhatlong/raft/storage"
)

// Want to make the system more robust and easy to change, might have many way to save the log of the node. But need to follwo the rule
// Log util method is directly use in the consesus algorithm for RequestVote and AppendEntries should not change the rule (can change the implementation)

/****************************************** Log utils function ***************************************************/
// Rule: Should return the term of a index and the slice of log from next index up to end of the log
func (cm *ConsensusModule) getTermAndSliceForIndex(prevLogIndex int) (int, []storage.Log) {
	return cm.Storage.GetTermAndSliceForIndex(prevLogIndex)
}

// Rule: Return a term of scecific index
func (cm *ConsensusModule) getTerm(index int) int {
	return cm.Storage.GetTerm(index)
}

// Rule: Returm the actual size of log (Both store in stable or RAM)
func (cm *ConsensusModule) getLogSize() int {
	return cm.Storage.GetLogSize()
}

// Return start of the index that in the log (in RAM)
func (cm *ConsensusModule) getLogStartIndex() int {
	return cm.Storage.GetLogStartIndex()
}

// Rule: return a slice (might query from stable storage, need to balance this)
func (cm *ConsensusModule) getLogSlice(from int, to int) []storage.Log {
	return cm.Storage.GetLogSlice(from, to)
}

// For Request vote
// Return the lastIndex and last term
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	return cm.Storage.LastLogIndexAndTerm()
}

func (cm *ConsensusModule) appendLog(slice []storage.Log) {
	cm.Storage.AppendLog(slice)
}

func (cm *ConsensusModule) getLog() []storage.Log {
	return cm.Storage.GetLog()
}

func (cm *ConsensusModule) UpdateLog(newLog []storage.Log) {
	cm.Storage.UpdateLog(newLog)
}

func (cm *ConsensusModule) GetCurrentTerm() int {
	return cm.Storage.GetCurrentTerm()
}

func (cm *ConsensusModule) UpdateCurrentTerm(newTerm int) {
	cm.Storage.UpdateCurrentTerm(newTerm)
}

func (cm *ConsensusModule) VoteFor(peerId int) {
	cm.Storage.VoteFor(peerId)
	//cm.Storage.votedFor = peerId
}

func (cm *ConsensusModule) GetVoteFor() int {
	return cm.Storage.GetVoteFor()
}

func (cm *ConsensusModule) GetLastIncludedIndex() int {
	return cm.Storage.GetLastIncludedIndex()
}

func (cm *ConsensusModule) UpdateLastIncludedIndex(newIncludedIndex int) {
	cm.Storage.UpdateLastIncludedIndex(newIncludedIndex)
}

func (cm *ConsensusModule) GetLastIncludedTerm() int {
	return cm.Storage.GetLastIncludedIndex()
}

func (cm *ConsensusModule) UpdateLastIncludedTerm(newIncludedTerm int) {
	cm.Storage.UpdateLastIncludedTerm(newIncludedTerm)
}

func (cm *ConsensusModule) GetCommitIndex() int {
	return cm.Storage.GetCommitIndex()
}

func (cm *ConsensusModule) UpdateCommitIndex(newCommitIndex int) {
	cm.Storage.UpdateCommitIndex(newCommitIndex)
}

func (cm *ConsensusModule) GetLastApplied() int {
	return cm.Storage.GetLastApplied()
}

func (cm *ConsensusModule) UpdateLastApplied(newLastApplied int) {
	cm.Storage.UpdateLastApplied(newLastApplied)
}

func (cm *ConsensusModule) GetMatchIndex(peerId int) int {
	return cm.Storage.GetMatchIndex(peerId)
}

func (cm *ConsensusModule) UpdateMatchIndex(peerId int, newMatchIndex int) {
	cm.Storage.UpdateMatchIndex(peerId, newMatchIndex)
}

func (cm *ConsensusModule) GetNextIndex(peerId int) int {
	return cm.Storage.GetNextIndex(peerId)
}

func (cm *ConsensusModule) UpdateNextIndex(peerId int, newNextIndex int) {
	cm.Storage.UpdateNextIndex(peerId, newNextIndex)
}

func (cm *ConsensusModule) GetMatchIncludedIndex(peerId int) int {
	return cm.Storage.GetMatchIncludedIndex(peerId)
}

func (cm *ConsensusModule) UpdateMatchIncludedIndex(peerId int, newMatchIncludedIndex int) {
	cm.Storage.UpdateMatchIncludedIndex(peerId, newMatchIncludedIndex)
}

func (cm *ConsensusModule) GetAllNextIndex() map[int]int {
	return cm.Storage.GetAllNextIndex()
}

func (cm *ConsensusModule) GetAllMatchIndex() map[int]int {
	return cm.Storage.GetAllMatchIndex()
}

func (cm *ConsensusModule) GetAllMatchIncludedIndex() map[int]int {
	return cm.Storage.GetAllMatchIncludedIndex()
}

func (cm *ConsensusModule) GetSnapshot(fileIndex int) storage.Snapshot {
	return cm.Storage.GetSnapshot(fileIndex)
}

func (cm *ConsensusModule) TakeInstallSnapshot(snapshot storage.Snapshot) {
	cm.Storage.TakeInstallSnapshot(snapshot)
}

func (cm *ConsensusModule) TakeSnapshot() {
	cm.Storage.TakeSnapshot()
}
