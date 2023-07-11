package raft

import "time"

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
	cm.debugLog("RequestVote response for candidate %d: %+v", args.CandidateId, response)
	return nil
}

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
	cm.debugLog("Become Candidate (currentTerm=%d); lastIncludeIndex=%d log=%+v", currentTerm,cm.lastIncludedIndex, cm.log)
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
				cm.debugLog("received RequestVoteResponse from peer %d %+v", peerId, response)
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
