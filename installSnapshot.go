package raft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

const SNAPSHOT_LOGSIZE = 32

type Snapshot struct {
	LastIncludedIndex int   `json:"lastIncludedIndex"`
	LastIncludedTerm  int   `json:"lastIncludedTerm"`
	Logs              []Log `json:"log"`
}

// Modify from the paper
// Explain and proof will be given
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              Snapshot
}

type InstallSnapshotResponse struct {
	Term              int
	LastIncludedIndex int
}

// Receiver implementation
func (cm *ConsensusModule) InstallSnapshot(args InstallSnapshotArgs, response *InstallSnapshotResponse) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	currentTerm := cm.currentTerm
	id := cm.id
	cm.debugLog("Receive InstallSnapshot: %+v", args)
	if args.Term > currentTerm {
		cm.revertToFollower(args.Term)
	}
	cm.electionTimeoutReset = time.Now()
	// Get information only
	if args.Offset == 0 || len(args.Data.Logs) == 0{
		response.Term = currentTerm
		response.LastIncludedIndex = cm.lastIncludedIndex
		return nil
	}
	if args.LastIncludedIndex < cm.lastIncludedIndex {
		response.Term = currentTerm
		response.LastIncludedIndex = cm.lastIncludedIndex
		return nil
	}
	cm.debugLog("Valid snapshot send with %+v", args)
	// Save snapshot to file here (dont accquire the lock when do this) consider create a read/write system lock only
	TakeInstallSnapshot(args.Data, id)
	cm.lastIncludedIndex = args.Data.LastIncludedIndex
	cm.lastIncludedTerm = args.Data.LastIncludedTerm
	cm.debugLog("Snap shot from data %+v",args.Data)
	cm.debugLog("Change in last Include index: %v", cm.lastIncludedIndex)
	cm.applyStateMachineEvent <- struct{}{}
	cm.debugLog("Install snapshot with include index %v and include term %v", cm.lastIncludedIndex, cm.lastIncludedTerm)
	return nil
}

// Sender implementation (require leader)
func (cm *ConsensusModule) sendInstallSnapshot() {
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
			var args InstallSnapshotArgs
			// Do not know the index or index too close
			if cm.matchIncludedIndex[peerId] == 0 || (cm.lastIncludedIndex-cm.matchIncludedIndex[peerId] <= 2*SNAPSHOT_LOGSIZE) {
				snapshot := Snapshot{}
				args = InstallSnapshotArgs{
					Term:              currentTerm,
					LeaderId:          cm.id,
					LastIncludedIndex: cm.lastIncludedIndex,
					LastIncludedTerm:  cm.lastIncludedTerm,
					Offset:            cm.matchIncludedIndex[peerId],
					Data:              snapshot,
				}
			} else {
				filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, cm.matchIncludedIndex[peerId]/SNAPSHOT_LOGSIZE+1)
				datasnapshot, err := GetSnapshot(filename)
				if err != nil{
					cm.debugLog("Error when read snapshot from file")
				}
				args = InstallSnapshotArgs{
					Term:              currentTerm,
					LeaderId:          cm.id,
					LastIncludedIndex: cm.lastIncludedIndex,
					LastIncludedTerm:  cm.lastIncludedTerm,
					Offset:            cm.matchIncludedIndex[peerId],
					Data:              datasnapshot,
				}
				cm.debugLog("Snap shot send to %d with data %+v",peerId,datasnapshot)
			}
			
			cm.debugLog("sending InstallSnapshot to %v: args=%+v", peerId, args)
			var response InstallSnapshotResponse
			cm_server := cm.server
			cm.mu.Unlock()
			if err := cm_server.Call(peerId, "ConsensusModule.InstallSnapshot", args, &response); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if response.Term > cm.currentTerm {
					cm.revertToFollower(response.Term)
				}
				cm.matchIncludedIndex[peerId] = response.LastIncludedIndex
				cm.debugLog("Update peer %d lastIncludedIndex %d", peerId, response.LastIncludedIndex)
			} else {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.debugLog("Error when send RPC ", err)
			}
		}(peerId)
	}
}

// Do the install snapshot that receive from leader
func TakeInstallSnapshot(snapshot Snapshot, id int) {
	dirPath := fmt.Sprintf("snapshot/server%d", id)
	makeDirIfNotExist(dirPath)
	filename := fmt.Sprintf("snapshot/server%d/%d.json", id, snapshot.LastIncludedIndex/SNAPSHOT_LOGSIZE)
	SaveSnapshot(snapshot, filename)

}

// Expect to get the lock from applyStateMachine (this operation will be very quick)
func (cm *ConsensusModule) TakeSnapshot() {

	lastIncludedIndex := cm.lastIncludedIndex + SNAPSHOT_LOGSIZE
	lastIncludedTerm := cm.log[cm.lastIncludedIndex+SNAPSHOT_LOGSIZE-1].Term
	logs := cm.log[cm.lastIncludedIndex : cm.lastIncludedIndex+SNAPSHOT_LOGSIZE]

	snapshot := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Logs:              logs,
	}
	dirPath := fmt.Sprintf("snapshot/server%d", cm.id)
	makeDirIfNotExist(dirPath)
	filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, lastIncludedIndex/SNAPSHOT_LOGSIZE)
	SaveSnapshot(snapshot, filename)
	cm.lastIncludedIndex = lastIncludedIndex
	cm.lastIncludedTerm = lastIncludedTerm
	cm.debugLog("Taking snapshot with include index %v and include term %v", lastIncludedIndex, lastIncludedTerm)
}

/************************************************************ Utils function but only for snapshot *************************************************/
func makeDirIfNotExist(dirPath string) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		// Create the directory
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			log.Fatal("Failed to create directory:", err)
		}
	}
}
func SaveSnapshot(snapshot Snapshot, filename string) error {
	snapShotdata, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, snapShotdata, 0644)
	return err
}

func GetSnapshot(filename string) (Snapshot, error) {
	snapshot := Snapshot{}
	jsonData, err := ioutil.ReadFile(filename)
	if err != nil {
		return snapshot, err
	}

	err = json.Unmarshal(jsonData, &snapshot)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}
