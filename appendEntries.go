package raft

import (
	"math"
	"time"

	"github.com/nhatlong/raft/storage"
)

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []storage.Log
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

	cm.debugLog("Receive AppendEntries: %+v", args)

	// Revert to follower if term is not up to date
	if args.Term > cm.GetCurrentTerm() {
		cm.revertToFollower(args.Term)
	}

	// Heartbeat to reset timeout
	cm.electionTimeoutReset = time.Now()

	// Return false if request term < currentTerm (5.1)
	if args.Term < cm.GetCurrentTerm() {
		response.Success = false
	} else {
		// All entries are different
		if args.PrevLogIndex == -1 {
			// Replace the whole current log by the master log entries
			//cm.log = append(cm.log[:0], args.Entries...)
			cm.appendLog(args.Entries)
			response.Success = true
			//cm.debugLog(cm.id,("New log update: %v", cm.log)
			cm.debugLog("New log update: %v")
			// Set commit index
			if args.LeaderCommit > cm.GetCommitIndex() {
				cm.UpdateCommitIndex(int(math.Min(float64(args.LeaderCommit), float64(cm.getLogSize()-1))))
				//cm.GetCommitIndex() = int(math.Min(float64(args.LeaderCommit), float64(cm.getLogSize()-1)))
				cm.debugLog("Change in commit index: %v", cm.GetCommitIndex())
				cm.applyStateMachineEvent <- struct{}{}
				cm.debugLog("Finish apply commit %v to state machine", cm.GetCommitIndex())
			}
		} else {
			// The server log does not have index at PrevLogIndex (An outdate log)
			if args.PrevLogIndex >= cm.getLogSize() {
				response.Success = false
			} else {
				// The term at PrevLogIndex are different with leader
				if args.PrevLogTerm != cm.getTerm(args.PrevLogIndex) {
					response.Success = false
				} else {

					// Sucess case need to divide to 2
					// Case 1 the PreviousLogIndex is not in the RAM (actual machine)
					if args.PrevLogIndex+1 < cm.getLogStartIndex() {
						// Update the log and return true (off course)
						// replaace all the cmlog
						//cm.log = args.Entries[cm.getLogStartIndex()-(args.PrevLogIndex+1):]
						cm.UpdateLog(args.Entries[cm.getLogStartIndex()-(args.PrevLogIndex+1):])
						response.Success = true
					} else { //Case 2 is when PrevLogIndex is in the RAM (actual machine)
						// Find the matching index then replace from that upward
						cm.UpdateLog(append(cm.getLogSlice(cm.getLogStartIndex(), args.PrevLogIndex+1), args.Entries...))
						//cm.log = append(cm.getLogSlice(cm.getLogStartIndex(), args.PrevLogIndex+1), args.Entries...)
						cm.debugLog("New log update: %v", cm.getLog())
						response.Success = true
					}
					// Set commit index
					if args.LeaderCommit > cm.GetCommitIndex() {
						//cm.GetCommitIndex() = int(math.Min(float64(args.LeaderCommit), float64(cm.getLogSize()-1)))
						cm.UpdateCommitIndex(int(math.Min(float64(args.LeaderCommit), float64(cm.getLogSize()-1))))
						cm.debugLog("Change in commit index: %v", cm.GetCommitIndex())
						cm.applyStateMachineEvent <- struct{}{}
						cm.debugLog("Finish apply commit %v to state machine", cm.GetCommitIndex())
					}
				}
			}
		}
	}
	// Common response state
	response.Term = cm.GetCurrentTerm()
	cm.debugLog("AppendEntries response: %+v", *response)
	cm.debugLog("End of append entries")
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

	currentTerm := cm.GetCurrentTerm()
	peerIds := cm.peerIds
	cm.mu.Unlock()

	for _, peerId := range peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			nextIndex := cm.GetNextIndex(peerId)
			prevLogIndex := nextIndex - 1

			prevLogTerm, entries := cm.getTermAndSliceForIndex(prevLogIndex)

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.GetCommitIndex(),
			}
			cm.debugLog("sending AppendEntries to %v: nextIndex=%d, args=%+v", peerId, nextIndex, args)
			var response AppendEntriesResponse
			cm_server := cm.server
			cm.mu.Unlock()
			if err := cm_server.Call(peerId, "ConsensusModule.AppendEntries", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if response.Term > cm.GetCurrentTerm() {
					cm.revertToFollower(response.Term)
					return
				}

				if cm.state == Leader && currentTerm == response.Term {
					// In case of success upadre the next index and match index
					if response.Success {
						cm.UpdateNextIndex(peerId, nextIndex+len(entries))
						//cm.nextIndex[peerId] = nextIndex + len(entries)
						cm.UpdateMatchIndex(peerId, cm.GetNextIndex(peerId)-1)
						//cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						commitIndex := cm.GetCommitIndex()
						// Commit all entries that have major agreement
						for i := cm.GetCommitIndex() + 1; i < cm.getLogSize(); i++ {
							if cm.getTerm(i) == cm.GetCurrentTerm() {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.GetMatchIndex(peerId) >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.UpdateCommitIndex(i)
								}
							}
						}
						cm.debugLog("AppendEntries response from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, cm.GetAllNextIndex(), cm.GetAllMatchIndex(), cm.GetCommitIndex())
						// Change in commit index (some command successfull get majority)
						if cm.GetCommitIndex() != commitIndex {
							cm.applyStateMachineEvent <- struct{}{}
						}
					} else {
						cm.UpdateNextIndex(peerId, cm.GetNextIndex(peerId)-1)
					}
				}
			}
		}(peerId)
	}
}
