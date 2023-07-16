package storage

type Log struct {
	Command string
	Term    int
}

type StorageInterface struct{
	stable StableStorage
	volatile VolatileStorage
}

/******************************************************** Util function for log ********************/
func (si *StorageInterface) GetTerm(index int) int {
	if index >= si.GetLastIncludedIndex() {
		//return si.
		return 0
	} else{
		return 0
	}
}

/****************************************** Get and set function for volatile ********************/
func (si *StorageInterface) GetCurrentTerm() int{
	return si.volatile.currentTerm
}

func (si *StorageInterface) UpdateCurrentTerm(newTerm int) {
	si.volatile.currentTerm = newTerm
}

func (si *StorageInterface) VoteFor(peerId int) {
	si.volatile.votedFor = peerId
}

func (si *StorageInterface) GetLastIncludedIndex() int {
	return si.volatile.lastIncludedIndex
}

func (si *StorageInterface) UpdateLastIncludedIndex(newIncludedIndex int) {
	si.volatile.lastIncludedIndex  = newIncludedIndex
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

func (si *StorageInterface) UpdateCommitIndex(newCommitIndex int){
	si.volatile.commitIndex = newCommitIndex
}

func (si *StorageInterface) GetLastApplied() int {
	return si.volatile.lastApplied
}

func (si *StorageInterface) UpdateLastApplied(newLastApplied int){
	si.volatile.lastApplied = newLastApplied
}

func (si *StorageInterface) GetMatchIndex(peerId int) int{
	return si.volatile.matchIndex[peerId]
}

func (si *StorageInterface) UpdateMatchIndex(peerId int,newMatchIndex int) {
	si.volatile.matchIndex[peerId] = newMatchIndex
}

func (si *StorageInterface) GetNextIndex(peerId int) int{
	return si.volatile.nextIndex[peerId]
}

func (si *StorageInterface) UpdateNextIndex(peerId int,newNextIndex int) {
	si.volatile.nextIndex[peerId] = newNextIndex
}

func (si *StorageInterface) GetMatchIncludedIndex(peerId int) int{
	return si.volatile.matchIncludedIndex[peerId]
}

func (si *StorageInterface) UpdateMatchIncludedIndex(peerId int,newMatchIncludedIndex int) {
	si.volatile.matchIncludedIndex[peerId] = newMatchIncludedIndex
}

func (si *StorageInterface) AppendCommand(command string, term int) {	
	si.volatile.log = append(si.volatile.log,Log{Command: command,Term: term})
}





