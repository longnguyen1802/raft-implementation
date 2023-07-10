package raft

import (
	"math"
	"time"
)

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
			cm.debugLog("New log update: %v", cm.log)
			// Set commit index
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(cm.log)-1)))
				cm.debugLog("Change in commit index: %v", cm.commitIndex)
				cm.applyStateMachineEvent <- struct{}{}
				cm.debugLog("Finish apply commit %v to state machine", cm.commitIndex)
			}
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
					cm.debugLog("New log update: %v", cm.log)
					response.Success = true
					response.Term = cm.currentTerm
					// Set commit index
					if args.LeaderCommit > cm.commitIndex {
						cm.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(cm.log)-1)))
						cm.debugLog("Change in commit index: %v", cm.commitIndex)
						cm.applyStateMachineEvent <- struct{}{}
						cm.debugLog("Finish apply commit %v to state machine", cm.commitIndex)
					}
				}
			}
		}
	}
	cm.debugLog("AppendEntries response: %+v", *response)
	return nil
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
	peerIds := cm.peerIds
	cm.mu.Unlock()

	for _, peerId := range peerIds {
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
			cm.debugLog("sending AppendEntries to %v: nextIndex=%d, args=%+v", peerId, nextIndex, args)
			var response AppendEntriesResponse
			cm_server := cm.server
			cm.mu.Unlock()
			if err := cm_server.Call(peerId, "ConsensusModule.AppendEntries", args, &response); err == nil {
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
