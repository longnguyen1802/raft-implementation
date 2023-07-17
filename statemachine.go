package raft

import (
	"github.com/nhatlong/raft/storage"
)

// Can expand a new statemachine
// Only expose api between consensus module and state machine here

// Nothing is really doing here just act as a state machine
func (cm *ConsensusModule) applyStateMachine() {
	for range cm.applyStateMachineEvent {
		cm.mu.Lock()
		cm.debugLog("Start apply statemachine")
		//var logs []Log
		// Check where signal come from
		// Come from commit no need to change the log
		cm.debugLog("Call apply state machine with commitIndex %d and lastIncludedIndex %d", cm.GetCommitIndex(), cm.GetLastIncludedIndex())
		if cm.GetCommitIndex() > cm.GetLastIncludedIndex()-1 {
			if cm.GetCommitIndex() > cm.GetLastApplied() {
				//logs = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
				cm.UpdateLastApplied(cm.GetCommitIndex())
			}
			cm.debugLog("Apply new commit %d to state machine", cm.GetLastApplied())
			// Apply to state machine can call a go routine function
			// Pending
			for {
				if cm.GetLastApplied() >= cm.GetLastIncludedIndex()+storage.SNAPSHOT_LOGSIZE {
					cm.TakeSnapshot()
				} else {
					break
				}
			}
		} else {
			// Read the snapshot Install and apply to statemachine
			snapshot := cm.GetSnapshot(cm.GetLastIncludedIndex() / storage.SNAPSHOT_LOGSIZE)
			cm.UpdateLastApplied(cm.GetLastIncludedIndex())

			//cm.lastApplied = cm.lastIncludedIndex
			cm.debugLog("Apply new commit %d by take snapshot %+v to state machine", cm.GetLastApplied(), snapshot)
			// Redo this
			cm.UpdateCommitIndex(cm.GetLastIncludedIndex())
		}
		cm.debugLog("End apply statemachine")
		cm.mu.Unlock()
	}
}
