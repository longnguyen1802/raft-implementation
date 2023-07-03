// Consensus module include everything related to Consensus
package raft

// TODO: Remove after debugging
var networkSeparate = make(map[int]bool)

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
)

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int

	server *server

	currentTerm int
	votedFor    int
	log         []LogEntry

	state CMState
	// Store the last election reset timestamp
	electionTimeoutReset time.Time
	// Fire the AppendEntries Event
	appendEntriesEvent chan struct{}
}

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
		// In leader state no timeout
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

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, response *RequestVoteResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()

	// Revert to follower if the term is not up to date
	if args.Term > cm.currentTerm {
		cm.revertToFollower(args.Term)
	}

	// If this server havent vote for anyone and the request server meet criteria
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		response.VoteGranted = true
		cm.votedFor = args.CandidateId
	} else {
		response.VoteGranted = false
	}
	response.Term = cm.currentTerm
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

	// Optimization
	ConflictIndex int
	ConflictTerm  int
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, response *AppendEntriesResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}

	// Revert to follower if term is not up to date
	if args.Term > cm.currentTerm {
		cm.revertToFollower(args.Term)
	}

	response.Success = false
	if args.Term == cm.currentTerm {
		// Revert to follower if in Candidate
		if cm.state != Follower {
			cm.revertToFollower(args.Term)
		}
		// Reset the election timeout
		cm.electionTimeoutReset = time.Now()

		// Do log matching
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find the insert point
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			}

			// Set commit index.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term
				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
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
