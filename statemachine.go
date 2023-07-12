package raft

import "fmt"

// Can expand a new statemachine
// Only expose api between consensus module and state machine here

// Nothing is really doing here just act as a state machine
func (cm *ConsensusModule) applyStateMachine() {
	for range cm.applyStateMachineEvent {
		cm.mu.Lock()
		//var logs []Log
		// Check where signal come from
		// Come from commit no need to change the log
		cm.debugLog("Call apply state machine with commitIndex %d and lastIncludedIndex %d", cm.commitIndex, cm.lastIncludedIndex)
		if cm.commitIndex > cm.lastIncludedIndex-1 {
			if cm.commitIndex > cm.lastApplied {
				//logs = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
				cm.lastApplied = cm.commitIndex
			}
			cm.debugLog("Apply new commit %d to state machine", cm.lastApplied)
			// Apply to state machine can call a go routine function
			// Pending
			for {
				if cm.lastApplied >= cm.lastIncludedIndex+SNAPSHOT_LOGSIZE {
					cm.TakeSnapshot()
				} else {
					break
				}
			}
		} else {
			filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, cm.lastIncludedIndex/SNAPSHOT_LOGSIZE)
			snapshot, err := GetSnapshot(filename)
			if err != nil {
				cm.debugLog("Got error when load snapshot", err)
			}
			cm.lastApplied = cm.lastIncludedIndex
			cm.debugLog("Apply new commit %d by take snapshot %+v to state machine", cm.lastApplied, snapshot)
			// Redo this
			// if cm.getLogSize() < cm.lastIncludedIndex{
			// 	extendedArray :=  make([]Log, cm.lastIncludedIndex)
			// 	copy(extendedArray, cm.log[:])
			// 	cm.log = extendedArray
			// }
			// copy(cm.log[cm.lastIncludedIndex-SNAPSHOT_LOGSIZE:cm.lastIncludedIndex], snapshot.Logs)
			cm.commitIndex = cm.lastIncludedIndex
		}
		cm.mu.Unlock()
	}
}
