package storage

type VolatileStorage struct {

	// Persistent state of Server
	lastIncludedIndex int
	lastIncludedTerm  int

	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state of Server
	commitIndex int
	lastApplied int
	// Volatile leader state
	// For append entries
	nextIndex  map[int]int
	matchIndex map[int]int
	// For snapshot
	matchIncludedIndex map[int]int
}

func NewVolatileStorage() *VolatileStorage {
	vs := new(VolatileStorage)
	vs.votedFor = -1
	vs.commitIndex = -1
	vs.lastApplied = -1
	// For easier calculation lastIncludeIndex will be the number of entry log already save
	vs.lastIncludedIndex = 0
	vs.lastIncludedTerm = -1

	vs.nextIndex = make(map[int]int)
	vs.matchIndex = make(map[int]int)

	vs.matchIncludedIndex = make(map[int]int)
	return vs
}

func (vs *VolatileStorage) GetTerm(index int) int {
	return vs.log[index-vs.lastIncludedIndex].Term
}

func (vs *VolatileStorage) GetLogSlice(from int, to int) []Log {
	return vs.log[from-vs.lastIncludedIndex : to-vs.lastIncludedIndex]
}
