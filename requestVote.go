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

