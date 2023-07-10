package raft

// Can expand a new statemachine
// Only expose api between consensus module and state machine here

// Nothing is really doing here just act as a state machine
func (cm *ConsensusModule) applyStateMachine() {
	for range cm.applyStateMachineEvent {
		cm.mu.Lock()
		//var logs []Log
		if cm.commitIndex > cm.lastApplied {
			//logs = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.debugLog("Apply new commit %d to state machine", cm.lastApplied)
		// Apply to state machine can call a go routine function
		// Pending
		cm.mu.Unlock()
	}
}
