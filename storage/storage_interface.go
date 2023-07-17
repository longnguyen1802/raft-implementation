package storage

import (
	"fmt"
	"log"
	"os"
)

type Log struct {
	Command string
	Term    int
}

type StorageInterface struct {
	stable   *StableStorage
	volatile *VolatileStorage
}

func NewStorageInterface(serverId int) *StorageInterface {
	si := new(StorageInterface)
	si.stable = NewStableStorage(serverId)
	si.volatile = NewVolatileStorage()
	return si
}

/******************************************************** Util function for log ********************/
// For append entries
func (si *StorageInterface) GetTermAndSliceForIndex(prevLogIndex int) (int, []Log) {
	if prevLogIndex >= 0 {
		return si.GetTerm(prevLogIndex), si.GetLogSlice(prevLogIndex+1, si.GetLogSize())
	} else {
		return -1, si.volatile.log
	}
}

func (si *StorageInterface) GetTerm(index int) int {
	if index >= si.GetLastIncludedIndex() {
		return si.volatile.GetTerm(index)
	} else {
		return si.stable.GetTerm(index)
	}
}

func (si *StorageInterface) GetLogSlice(from int, to int) []Log {
	if from >= si.GetLastIncludedIndex() {
		return si.volatile.GetLogSlice(from, to)
	}
	if to <= si.GetLastIncludedIndex() {
		return si.stable.GetLogSlice(from, to)
	}

	logSlice := make([]Log, to-from)
	volatile_log := si.GetLogSlice(si.GetLastIncludedIndex(), to)
	stable_log := si.GetLogSlice(from, si.GetLastIncludedIndex())

	copy(logSlice[0:len(stable_log)], stable_log)
	copy(logSlice[len(stable_log):], volatile_log)

	return logSlice
}

// The actual log size
func (si *StorageInterface) GetLogSize() int {
	return si.volatile.lastIncludedIndex + len(si.volatile.log)
}

// Return start of the index that in the log (in RAM)
func (si *StorageInterface) GetLogStartIndex() int {
	return si.volatile.lastIncludedIndex
}

// For Request vote
// Return the lastIndex and last term
func (si *StorageInterface) LastLogIndexAndTerm() (int, int) {
	if si.GetLogSize() > 0 {
		lastIndex := si.GetLogSize() - 1
		return lastIndex, si.GetTerm(lastIndex)
	} else {
		return -1, -1
	}
}

/****************************************** Get and set function for volatile ********************/
func (si *StorageInterface) GetCurrentTerm() int {
	return si.volatile.currentTerm
}

func (si *StorageInterface) UpdateCurrentTerm(newTerm int) {
	si.volatile.currentTerm = newTerm
}

func (si *StorageInterface) VoteFor(peerId int) {
	si.volatile.votedFor = peerId
}

func (si *StorageInterface) GetVoteFor() int {
	return si.volatile.votedFor
}

func (si *StorageInterface) GetLastIncludedIndex() int {
	return si.volatile.lastIncludedIndex
}

func (si *StorageInterface) UpdateLastIncludedIndex(newIncludedIndex int) {
	si.volatile.lastIncludedIndex = newIncludedIndex
}

func (si *StorageInterface) GetLastIncludedTerm() int {
	return si.volatile.lastIncludedTerm
}

func (si *StorageInterface) UpdateLastIncludedTerm(newIncludedTerm int) {
	si.volatile.lastIncludedTerm = newIncludedTerm
}

func (si *StorageInterface) GetCommitIndex() int {
	return si.volatile.commitIndex
}

func (si *StorageInterface) UpdateCommitIndex(newCommitIndex int) {
	si.volatile.commitIndex = newCommitIndex
}

func (si *StorageInterface) GetLastApplied() int {
	return si.volatile.lastApplied
}

func (si *StorageInterface) UpdateLastApplied(newLastApplied int) {
	si.volatile.lastApplied = newLastApplied
}

func (si *StorageInterface) GetMatchIndex(peerId int) int {
	return si.volatile.matchIndex[peerId]
}

func (si *StorageInterface) UpdateMatchIndex(peerId int, newMatchIndex int) {
	si.volatile.matchIndex[peerId] = newMatchIndex
}

func (si *StorageInterface) GetNextIndex(peerId int) int {
	return si.volatile.nextIndex[peerId]
}

func (si *StorageInterface) UpdateNextIndex(peerId int, newNextIndex int) {
	si.volatile.nextIndex[peerId] = newNextIndex
}

func (si *StorageInterface) GetMatchIncludedIndex(peerId int) int {
	return si.volatile.matchIncludedIndex[peerId]
}

func (si *StorageInterface) UpdateMatchIncludedIndex(peerId int, newMatchIncludedIndex int) {
	si.volatile.matchIncludedIndex[peerId] = newMatchIncludedIndex
}

func (si *StorageInterface) AppendCommand(command string, term int) {
	si.volatile.log = append(si.volatile.log, Log{Command: command, Term: term})
}

func (si *StorageInterface) GetLog() []Log {
	return si.volatile.log
}

func (si *StorageInterface) UpdateLog(newLog []Log) {
	si.volatile.log = newLog
}

func (si *StorageInterface) AppendLog(slice []Log) {
	si.volatile.log = append(si.volatile.log, slice...)
}

func (si *StorageInterface) GetAllNextIndex() map[int]int {
	return si.volatile.nextIndex
}

func (si *StorageInterface) GetAllMatchIndex() map[int]int {
	return si.volatile.matchIndex
}

func (si *StorageInterface) GetAllMatchIncludedIndex() map[int]int {
	return si.volatile.matchIncludedIndex
}

func (si *StorageInterface) GetSnapshot(fileIndex int) Snapshot {
	return si.stable.GetSnapshot(fileIndex)
}

// Do the install snapshot that receive from leader
func (si *StorageInterface) TakeInstallSnapshot(snapshot Snapshot) {
	makeDirIfNotExist("snapshot")
	dirPath := fmt.Sprintf("snapshot/server%d", si.stable.serverId)
	makeDirIfNotExist(dirPath)

	if len(si.GetLog()) < SNAPSHOT_LOGSIZE {
		si.UpdateLog(si.GetLog()[len(si.GetLog()):])
	} else {
		si.UpdateLog(si.GetLog()[SNAPSHOT_LOGSIZE:])
	}

	SaveSnapshotToFile(snapshot, si.stable.GetFileName(snapshot.LastIncludedIndex/SNAPSHOT_LOGSIZE))

	si.UpdateLastIncludedIndex(snapshot.LastIncludedIndex)
	si.UpdateLastIncludedTerm(snapshot.LastIncludedTerm)

}

// Expect to get the lock from applyStateMachine (this operation will be very quick)
func (si *StorageInterface) TakeSnapshot() {

	lastIncludedIndex := si.GetLastIncludedIndex() + SNAPSHOT_LOGSIZE
	// lastIncludedTerm := cm.log[cm.lastIncludedIndex+SNAPSHOT_LOGSIZE-1].Term
	lastIncludedTerm := si.GetTerm(si.GetLastIncludedIndex() + SNAPSHOT_LOGSIZE - 1)
	logs := si.GetLogSlice(si.GetLastIncludedIndex(), si.GetLastIncludedIndex()+SNAPSHOT_LOGSIZE)
	//cm.log[cm.lastIncludedIndex : cm.lastIncludedIndex+SNAPSHOT_LOGSIZE]
	// Truncate the log
	si.UpdateLog(si.GetLog()[SNAPSHOT_LOGSIZE:])

	snapshot := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Logs:              logs,
	}
	makeDirIfNotExist("snapshot")
	dirPath := fmt.Sprintf("snapshot/server%d", si.stable.serverId)
	makeDirIfNotExist(dirPath)
	SaveSnapshotToFile(snapshot, si.stable.GetFileName(lastIncludedIndex/SNAPSHOT_LOGSIZE))
	si.UpdateLastIncludedIndex(snapshot.LastIncludedIndex)
	si.UpdateLastIncludedTerm(snapshot.LastIncludedTerm)
}

func makeDirIfNotExist(dirPath string) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		// Create the directory
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			log.Fatal("Failed to create directory:", err)
		}
	}
}

// func (si *StorageInterface) debugLog(format string, args ...interface{}) {
// 	debug.DebugLog(si.stable.serverId, format, args)
// }
