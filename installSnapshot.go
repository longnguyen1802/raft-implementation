package raft

import (
	"time"

	"github.com/nhatlong/raft/storage"
)

// Modify from the paper
// Explain and proof will be given
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              storage.Snapshot
}

type InstallSnapshotResponse struct {
	Term              int
	LastIncludedIndex int
}

// Receiver implementation
func (cm *ConsensusModule) InstallSnapshot(args InstallSnapshotArgs, response *InstallSnapshotResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	currentTerm := cm.GetCurrentTerm()
	cm.debugLog("Receive InstallSnapshot: %+v", args)
	if args.Term > currentTerm {
		cm.revertToFollower(args.Term)
	}
	cm.electionTimeoutReset = time.Now()
	if args.Offset == 0 || len(args.Data.Logs) == 0 {
		response.Term = currentTerm
		response.LastIncludedIndex = cm.GetLastIncludedIndex()
		cm.debugLog("End of install snapshot")
		return nil
	}
	if args.Data.LastIncludedIndex <= cm.GetLastIncludedIndex() {
		response.Term = currentTerm
		response.LastIncludedIndex = cm.GetLastIncludedIndex()
		cm.debugLog("End of install snapshot")
		return nil
	}
	cm.debugLog("Valid snapshot send with %+v", args)
	// Save snapshot to file here (dont accquire the lock when do this) consider create a read/write system lock only
	cm.TakeInstallSnapshot(args.Data)
	cm.applyStateMachineEvent <- struct{}{}
	cm.debugLog("Install snapshot with include index %d and include term %d", cm.GetLastIncludedIndex(), cm.GetLastIncludedTerm())
	// Update term and last included index
	response.Term = cm.GetCurrentTerm()
	response.LastIncludedIndex = cm.GetLastIncludedIndex()
	cm.debugLog("End of install snapshot")
	return nil
}

// Sender implementation (require leader)
func (cm *ConsensusModule) sendInstallSnapshot() {
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
			var args InstallSnapshotArgs
			// Do not know the index or index too close
			if cm.GetMatchIncludedIndex(peerId) == 0 || (cm.GetLastIncludedIndex()-cm.GetMatchIncludedIndex(peerId) <= 2*storage.SNAPSHOT_LOGSIZE) {
				snapshot := storage.Snapshot{}
				args = InstallSnapshotArgs{
					Term:              currentTerm,
					LeaderId:          cm.id,
					LastIncludedIndex: cm.GetLastIncludedIndex(),
					LastIncludedTerm:  cm.GetLastIncludedTerm(),
					Offset:            cm.GetMatchIncludedIndex(peerId),
					Data:              snapshot,
				}
			} else {
				datasnapshot := cm.GetSnapshot(cm.GetMatchIncludedIndex(peerId)/storage.SNAPSHOT_LOGSIZE + 1)
				//filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, cm.matchIncludedIndex[peerId]/SNAPSHOT_LOGSIZE+1)
				// datasnapshot, err := GetSnapshot(filename)
				// if err != nil {
				// 	cm.debugLog("Error when read snapshot from file")
				// }
				args = InstallSnapshotArgs{
					Term:              currentTerm,
					LeaderId:          cm.id,
					LastIncludedIndex: cm.GetLastIncludedIndex(),
					LastIncludedTerm:  cm.GetLastIncludedTerm(),
					Offset:            cm.GetMatchIncludedIndex(peerId),
					Data:              datasnapshot,
				}
				cm.debugLog("Snap shot send to %d with data %+v", peerId, datasnapshot)
			}

			cm.debugLog("sending InstallSnapshot to %d: args=%+v", peerId, args)
			var response InstallSnapshotResponse
			cm_server := cm.server
			cm.mu.Unlock()
			if err := cm_server.Call(peerId, "ConsensusModule.InstallSnapshot", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if response.Term > cm.GetCurrentTerm() {
					cm.revertToFollower(response.Term)
				}
				//cm.matchIncludedIndex[peerId] = response.LastIncludedIndex
				cm.UpdateMatchIncludedIndex(peerId, response.LastIncludedIndex)
				cm.debugLog("Update peer %d lastIncludedIndex %d", peerId, response.LastIncludedIndex)
			} else {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("Error when send RPC ", err)
			}
		}(peerId)
	}
}
