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

func (vs *VolatileStorage) GetTerm(index int) int{
	return vs.log[index].Term
}
