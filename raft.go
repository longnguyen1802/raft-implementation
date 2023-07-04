// Consensus module include everything related to Consensus
package raft

import "math"
type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	// Only for debugging - Simulate a Failure machine (Both machine fail or network separate)
	Failure
)

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int

	server *Server

	// Persistent state of Server
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state of Server
	commitIndex int
	lastApplied int
	// State of server
	state CMState
	// Store the last election reset timestamp
	electionTimeoutReset time.Time

	// Event fire by client
	appendEntriesEvent chan struct{}
	commitEvent        chan struct{}

	// Volatile leader state
	nextIndex  map[int]int
	matchIndex map[int]int
}

/******************************************* Common Function ************************************/
func (cm *ConsensusModule) electionTimeout() {
	timeoutDuration := cm.timeoutDuration()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	// Run a ticker timer
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		// In leader and failure no timeout
		if cm.state == Leader || cm.state == Failure {
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
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionTimeoutReset = time.Now()

	go cm.electionTimeout()
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

	// Revert to follower if the term is not up to date (Rules for Servers)
	if args.Term > cm.currentTerm {
		cm.revertToFollower(args.Term)
	}

	// If request term < currentTerm return false (5.1)
	if args.Term < cm.currentTerm {
		response.VoteGranted = false
		response.term = cm.currentTerm
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
			response.term = cm.currentTerm
		}
	}
	cm.persistToStorage()
	return nil
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
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
	if cm.state == Failure {
		return nil
	}

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
			cm.log = append(cm.log[:0],args.Entries...)
			response.Success = true
			response.Term = cm.currentTerm
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
					cm.log = append(cm.log[:args.PrevLogIndex+1],args.Entries...)
					response.Success = true
					response.Term = cm.currentTerm
					// Set commit index
					if args.LeaderCommit > cm.commitIndex {
						cm.commitIndex = int(math.Min(args.LeaderCommit, len(cm.log)-1))
						cm.commitEvent <- struct{}{}
					}
				}
			}
		}
	}
	cm.persistToStorage()
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
	cm.voteFor = cm.id
	// Keep the number of vote
	numVoteReceived := 1
	// Send RequestVote RPCs to all peers

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

			var response RequestVoteResponse

			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

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
		// Immediately send AEs to peers.
		cm.appendEntries()

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
			case _, ok := <-cm.appendEntriesEvent:
				// Trigger by another event
				if ok {
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

			if doSend {
				// Stop when step down
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.appendEntries()
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
			var response AppendEntriesResponse
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if response.Term > cm.currentTerm {
					cm.revertToFollower(response.Term)
					return
				}

				if cm.state == Leader && currentTerm == reply.Term {
					// In case of success upadre the next index and match index
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
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

						// Change in commit index (Client Request) notify by sending appendEntries
						if cm.commitIndex != commitIndex {
							cm.newCommitReadyChan <- struct{}{}
							cm.appendEntriesEvent <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								cm.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								cm.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
					}
				}
			}
		}(peerId)
	}
}

/***********************************  Utility function *****************************************************/
// According to the paper setting timeout to 150ms - 300ms

func (cm *ConsensusModule) timeoutDuration() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}
