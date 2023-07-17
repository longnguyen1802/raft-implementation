// Consensus module include everything related to Consensus
package raft

import (
	"sync"
	"time"

	"github.com/nhatlong/raft/storage"
)

// Consider moving this to elsewhere (It is actually the main data structure)

type ConsensusModule struct {
	mu sync.Mutex

	id      int // will equal to server id
	peerIds []int

	server *Server
	// State of server
	state CMState
	// Store the last election reset timestamp
	electionTimeoutReset time.Time

	// This help to force run AppendEntries when client send request (Instead of manual heartbeat)
	AppendEntriesEvent chan struct{}
	// This will enforce to run applyStateMachine when there is new commit log
	applyStateMachineEvent chan struct{}
	// Storage
	Storage *storage.StorageInterface
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.applyStateMachineEvent = make(chan struct{}, 16)
	cm.AppendEntriesEvent = make(chan struct{}, 1)
	cm.state = Follower
	cm.Storage = storage.NewStorageInterface(cm.id)
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
	// debug.DebugLog(cm.id, "Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.Storage.AppendCommand(command, cm.GetCurrentTerm())
		//cm.log = append(cm.log, Log{Command: command, Term: cm.currentTerm})
		// debug.DebugLog(cm.id, "New log update: %v", cm.getLog())
		cm.mu.Unlock()
		// Trigger append entries event
		cm.AppendEntriesEvent <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}
