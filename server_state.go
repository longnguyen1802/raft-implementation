package raft

/****** Description

This file contain all the state transistion of server. If there is mistake in a state debug here

****/
import (
	"time"
)

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

/****************************************** Become follower ************************************/
func (cm *ConsensusModule) revertToFollower(term int) {
	cm.debugLog("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionTimeoutReset = time.Now()

	go cm.electionTimeout()
}

/******************************************* Become Candidate ************************************/
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

/****************************************** Leader State ****************************************************/
// Run this loop as long as the server is not fail
func (cm *ConsensusModule) leaderLoop() {

	// Setting nextIndex and matchIndex for each peer.
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
		cm.matchIncludedIndex[peerId] = 0
	}
	ticker := time.NewTicker(50 * time.Millisecond)

	// Start a goroutine to receive ticks
	go func() {
		for range ticker.C {
			cm.sendInstallSnapshot()
		}
	}()

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
