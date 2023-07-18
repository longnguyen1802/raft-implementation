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
	Config       Configuration
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
	// Invalid leader
	if args.Term < cm.currentTerm {
		response.Success = false
	} else {
		// All entries are different
		// Sound a valid leader update config
		if !cm.config.Compare(&args.Config) {
			// Indicate that there is new config
			cm.config = &args.Config
			// Update server config
			cm.server.UpdateConfig(args.Config)
			// Update local config
			cm.peerIds = make([]int, 0)
			for _, i := range args.Config.MachineIds {
				if i != cm.id {
					cm.peerIds = append(cm.peerIds, i)
				}
			}
		}

		if args.PrevLogIndex == -1 {
			// Replace the whole current log by the master log entries
			cm.log = append(cm.log[:0], args.Entries...)
			response.Success = true
			cm.debugLog("New log update: %v", cm.log)
			// Set commit index
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(cm.getLogSize()-1)))
				cm.debugLog("Change in commit index: %v", cm.commitIndex)
				cm.applyStateMachineEvent <- struct{}{}
				cm.debugLog("Finish apply commit %v to state machine", cm.commitIndex)
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
						cm.log = args.Entries[cm.getLogStartIndex()-(args.PrevLogIndex+1):]
						response.Success = true
					} else { //Case 2 is when PrevLogIndex is in the RAM (actual machine)
						// Find the matching index then replace from that upward
						cm.log = append(cm.getLogSlice(cm.getLogStartIndex(), args.PrevLogIndex+1), args.Entries...)
						cm.debugLog("New log update: %v", cm.log)
						response.Success = true
					}
					// Set commit index
					if args.LeaderCommit > cm.commitIndex {
						cm.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(cm.getLogSize()-1)))
						cm.debugLog("Change in commit index: %v", cm.commitIndex)
						cm.applyStateMachineEvent <- struct{}{}
						cm.debugLog("Finish apply commit %v to state machine", cm.commitIndex)
					}
				}
			}
		}
	}
	// Common response state
	response.Term = cm.currentTerm
	cm.debugLog("AppendEntries response: %+v", *response)
	//cm.debugLog("Current cm state term := %d, commitIndex:= %d, lastIncludedIndex := %d",cm.currentTerm,cm.commitIndex,cm.lastIncludedIndex)
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

			prevLogTerm, entries := cm.getTermAndSliceForIndex(prevLogIndex)

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
				Config:       *cm.config,
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
						for i := cm.commitIndex + 1; i < cm.getLogSize(); i++ {
							if cm.getTerm(i) == cm.currentTerm {
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
					}
				}
			}
		}(peerId)
	}
}
