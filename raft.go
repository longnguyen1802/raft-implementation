// Consensus module include everything related to Consensus
package raft

import (
	"sync"
	"time"
)

// Consider moving this to elsewhere (It is actually the main data structure)
type Log struct {
	Command string
	Term    int
}

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int

	server *Server

	// Persistent state of Server
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state of Server
	commitIndex int
	lastApplied int

	// State of server
	state CMState
	// Store the last election reset timestamp
	electionTimeoutReset time.Time

	// This help to force run AppendEntries when client send request (Instead of manual heartbeat)
	AppendEntriesEvent chan struct{}
	// This will enforce to run applyStateMachine when there is new commit log
	applyStateMachineEvent chan struct{}

	// Volatile leader state
	nextIndex  map[int]int
	matchIndex map[int]int
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.applyStateMachineEvent = make(chan struct{}, 16)
	cm.AppendEntriesEvent = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	go func() {
		// Wait the start of server until setup all server
		<-ready
		cm.mu.Lock()
		cm.electionTimeoutReset = time.Now()
		cm.mu.Unlock()
		cm.electionTimeout()
	}()

	go cm.applyStateMachine()
	return cm
}

/******************************************** API to client *************************************/
// Client submit command to server (Only leader can handle this)
// Can do better by response the leader id but assume this will not happen
func (cm *ConsensusModule) SubmitCommand(command string) bool {
	cm.mu.Lock()
	cm.debugLog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, Log{Command: command, Term: cm.currentTerm})
		cm.debugLog("New log update: %v", cm.log)
		cm.mu.Unlock()
		// Trigger append entries event
		cm.AppendEntriesEvent <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}
