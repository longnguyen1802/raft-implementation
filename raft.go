// Consensus module include everything related to Consensus
package raft

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)
var loglock sync.Mutex
type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	// Only for debugging - Simulate a Failure machine (Both machine fail or network separate)
	//Failure
)
func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("unreachable")
	}
}

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

func NewConsensusModule(id int, peerIds []int, server *Server,ready <-chan interface{}) *ConsensusModule {
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

/******************************************* Common Function ************************************/
func (cm *ConsensusModule) electionTimeout() {
	timeoutDuration := cm.timeoutDuration()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.debugLog("Election timeout started (%v), term=%d", timeoutDuration, termStarted)
	// Run a ticker timer
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		// In leader no timeout
		if cm.state == Leader {
			cm.mu.Unlock()
			return
		}

		// Current term change quit timmer
		if termStarted != cm.currentTerm {
			cm.mu.Unlock()
			return
		}

		// For both Follower and Candidate start new Election if timeout
		if elapsed := time.Since(cm.electionTimeoutReset); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}
func (cm *ConsensusModule) revertToFollower(term int) {
	cm.debugLog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionTimeoutReset = time.Now()

	go cm.electionTimeout()
}

func (cm *ConsensusModule) applyStateMachine() {
	for range cm.applyStateMachineEvent {
		cm.mu.Lock()
		//var logs []Log
		if cm.commitIndex > cm.lastApplied {
			//logs = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.debugLog("Apply new commit %d to state machine",cm.lastApplied)
		// Apply to state machine can call a go routine function
		// Pending
		cm.mu.Unlock()
	}
}

// Different consensus state consider moving them separately
/************************************************Follower State******************************************/
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC for receiver implementation
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, response *RequestVoteResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Get the last log index and term of this server
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()

	cm.debugLog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)
	// Revert to follower if the term is not up to date (Rules for Servers)
	if args.Term > cm.currentTerm {
		cm.debugLog("Term out dated")
		cm.revertToFollower(args.Term)
	}

	// If request term < currentTerm return false (5.1)
	if args.Term < cm.currentTerm {
		response.VoteGranted = false
		response.Term = cm.currentTerm
	} else {
		// Check vote granted criteria
		if (cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
			(args.LastLogTerm > lastLogTerm ||
				(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			// Response result
			response.VoteGranted = true
			response.Term = cm.currentTerm
			// Set server state
			cm.votedFor = args.CandidateId
			// Reset timeout when vote for a candidate
			cm.electionTimeoutReset = time.Now()
		} else {
			response.VoteGranted = false
			response.Term = cm.currentTerm
		}
	}
	cm.debugLog("RequestVote response for candidate %d: %+v",args.CandidateId, response)
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// Receiver implementation
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, response *AppendEntriesResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// Consider no response from Failure machine
	// if cm.state == Failure {
	// 	return nil
	// }

	cm.debugLog("Receive AppendEntries: %+v", args)

	// Revert to follower if term is not up to date
	if args.Term > cm.currentTerm {
		cm.revertToFollower(args.Term)
	}

	// Heartbeat to reset timeout
	cm.electionTimeoutReset = time.Now()

	// Return false if request term < currentTerm (5.1)
	if args.Term < cm.currentTerm {
		response.Success = false
		response.Term = cm.currentTerm
	} else {
		// All entries are different
		if args.PrevLogIndex == -1 {
			// Replace the whole current log by the master log entries
			cm.log = append(cm.log[:0], args.Entries...)
			response.Success = true
			response.Term = cm.currentTerm
			cm.debugLog("New log update: %v",cm.log)
		} else {
			// The server log does not have index at PrevLogIndex (An outdate log)
			if args.PrevLogIndex >= len(cm.log) {
				response.Success = false
				response.Term = cm.currentTerm
			} else {
				// The term at PrevLogIndex are different with leader
				if args.PrevLogTerm != cm.log[args.PrevLogIndex].Term {
					response.Success = false
					response.Term = cm.currentTerm
				} else {
					// Find the matching index then replace from that upward
					cm.log = append(cm.log[:args.PrevLogIndex+1], args.Entries...)
					cm.debugLog("New log update: %v",cm.log)
					response.Success = true
					response.Term = cm.currentTerm
					// Set commit index
					if args.LeaderCommit > cm.commitIndex {
						cm.debugLog("New commit index is coming")
						cm.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(cm.log)-1)))
						cm.applyStateMachineEvent <- struct{}{}
					}
				}
			}
		}
	}
	cm.debugLog("AppendEntries response: %+v", *response)
	return nil
}

/*****************************************Candidate State************************************************/
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	// Note the current term for this election
	currentTerm := cm.currentTerm
	// Reset timeout when become Candidate
	cm.electionTimeoutReset = time.Now()
	// Vote for itself
	cm.votedFor = cm.id
	// Keep the number of vote
	numVoteReceived := 1
	// Send RequestVote RPCs to all peers
	cm.debugLog("Become Candidate (currentTerm=%d); log=%v", currentTerm, cm.log)
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  cm.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			cm.debugLog("sending RequestVote to %d: %+v", peerId, args)
			var response RequestVoteResponse

			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("received RequestVoteResponse from peer %d %+v",peerId, response)
				// In case it already win or already become follower
				if cm.state != Candidate {
					return
				}

				// Revert to follower if receive RPC with higher term
				if response.Term > currentTerm {
					cm.revertToFollower(response.Term)
					return
				} else if response.Term == currentTerm {
					if response.VoteGranted {
						numVoteReceived += 1
						if numVoteReceived*2 > len(cm.peerIds)+1 {
							// Won the election
							cm.debugLog("wins election with %d votes", numVoteReceived)
							cm.state = Leader
							cm.leaderLoop()
							return
						}
					}
				}
			}
		}(peerId)
	}
	// Run the electionTimeout in case it is not successful
	go cm.electionTimeout()
}

/****************************************** Leader State****************************************************/
// Run this loop as long as the server is not fail
func (cm *ConsensusModule) leaderLoop() {

	// Setting nextIndex and matchIndex for each peer.
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}

	// Send heartbeat (AppendEntries) every 50ms in background.
	// Also run when receive command from client or resend commit to other peers
	go func(heartbeatTimeout time.Duration) {
		// Immediately send Append Entries to peers.
		cm.sendAppendEntries()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			sending := false
			select {
			case <-t.C:
				sending = true
				// Reset timer when timeout
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.AppendEntriesEvent:
				// Trigger by another event
				if ok {
					cm.debugLog("Heartbeat got stopped by another event")
					sending = true
				} else {
					return
				}

				// Reset timer for heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if sending {
				// Stop when step down
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.sendAppendEntries()
			}
		}
	}(50 * time.Millisecond)
}

// only handle by leader send append entries to all Follower
func (cm *ConsensusModule) sendAppendEntries() {
	// Cancel if it is no longer leader
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}

	currentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			nextIndex := cm.nextIndex[peerId]
			prevLogIndex := nextIndex - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			// Send all entries from nextIndex
			entries := cm.log[nextIndex:]

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.debugLog("sending AppendEntries to %v: nextIndex=%d, args=%+v", peerId, nextIndex, args)
			var response AppendEntriesResponse
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if response.Term > cm.currentTerm {
					cm.revertToFollower(response.Term)
					return
				}

				if cm.state == Leader && currentTerm == response.Term {
					// In case of success upadre the next index and match index
					if response.Success {
						cm.nextIndex[peerId] = nextIndex + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						commitIndex := cm.commitIndex
						// Commit all entries that have major agreement
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						cm.debugLog("AppendEntries response from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)
						// Change in commit index (some command successfull get majority)
						if cm.commitIndex != commitIndex {
							cm.applyStateMachineEvent <- struct{}{}
							//cm.AppendEntriesEvent <- struct{}{}
						}
					} else {
						cm.nextIndex[peerId] -= 1
						cm.debugLog("AppendEntries response from %d not success: nextIndex := %d", peerId, nextIndex-1)
					}
				}
			}
		}(peerId)
	}
}
// Client submit command to server (Only leader can handle this) 
// Can do better by response the leader id but assume this will not happen
func (cm *ConsensusModule) SubmitCommand(command string) bool {
	cm.mu.Lock()
	cm.debugLog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log,Log{Command:command,Term:cm.currentTerm})
		cm.debugLog("New log update: %v",cm.log)
		cm.mu.Unlock()
		// Trigger append entries event
		cm.AppendEntriesEvent <- struct{}{}
		return true
	}
	cm.mu.Unlock()
	return false
}
/***********************************  Utility function *****************************************************/
// According to the paper setting timeout to 300ms - 600ms

func (cm *ConsensusModule) timeoutDuration() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (cm *ConsensusModule) debugLog(format string, args ...interface{}) {
	f, err := os.OpenFile("server"+strconv.Itoa(cm.id)+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
