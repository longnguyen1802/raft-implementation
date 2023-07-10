package raft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

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
	Data              []byte
}

type InstallSnapshotResponse struct {
	Term int
}

// Receiver implementation
func (cm *ConsensusModule) InstallSnapshot(args InstallSnapshotArgs, response *InstallSnapshotResponse) error {
	cm.mu.Lock()
	currentTerm := cm.currentTerm
	id := cm.id
	cm.debugLog("Receive InstallSnapshot: %+v", args)
	if args.Term > currentTerm {
		cm.revertToFollower(args.Term)
	}
	cm.electionTimeoutReset = time.Now()
	cm.mu.Unlock()
	// Save snapshot to file here (dont accquire the lock when do this) consider create a read/write system lock only
	var snapshot Snapshot
	err := json.Unmarshal(args.Data, &snapshot)
	if err != nil {
		return err
	}
	TakeInstallSnapshot(snapshot, id)
	cm.mu.Lock()
	cm.lastIncludedIndex = snapshot.LastIncludedIndex
	cm.lastIncludedTerm = snapshot.LastIncludedTerm
	cm.debugLog("Install snapshot with include index %v and include term %v", cm.lastIncludedIndex, cm.lastIncludedTerm)
	cm.mu.Unlock()
	return nil
}

// Sender implementation (require leader)
func (cm *ConsensusModule) sendInstallSnapshot(peerId int, peerLastIncludedIndex int) {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	currentTerm := cm.currentTerm
	cm.mu.Unlock()
	go func(peerId int, peerLastIncludedIndex int) {
		cm.mu.Lock()
		filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, peerLastIncludedIndex/256)
		snapshot, err := GetSnapshot(filename)
		if err != nil {
			log.Fatal("Failed to get data from JSON:", err)
		}
		data, err := json.Marshal(snapshot)
		if err != nil {
			log.Fatal("Failed to marshal JSON:", err)
		}
		args := InstallSnapshotArgs{
			Term:              currentTerm,
			LeaderId:          cm.id,
			LastIncludedIndex: snapshot.LastIncludedIndex,
			LastIncludedTerm:  snapshot.LastIncludedTerm,
			Data:              data,
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
				return
			}
		}
	}(peerId, peerLastIncludedIndex)

}

// Do the install snapshot that receive from leader
func TakeInstallSnapshot(snapshot Snapshot, id int) {
	dirPath := fmt.Sprintf("snapshot/server%d", id)
	makeDirIfNotExist(dirPath)
	filename := fmt.Sprintf("snapshot/server%d/%d.json", id, snapshot.LastIncludedIndex/256)
	SaveSnapshot(snapshot, filename)

}

// Expect to get the lock from applyStateMachine (this operation will be very quick)
func (cm *ConsensusModule) TakeSnapshot() {

	lastIncludedIndex := cm.lastIncludedIndex + 256
	lastIncludedTerm := cm.log[cm.lastIncludedIndex+255].Term
	logs := cm.log[cm.lastIncludedIndex : cm.lastIncludedIndex+256]

	snapshot := Snapshot{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Logs:              logs,
	}
	dirPath := fmt.Sprintf("snapshot/server%d", cm.id)
	makeDirIfNotExist(dirPath)
	filename := fmt.Sprintf("snapshot/server%d/%d.json", cm.id, lastIncludedIndex/256)
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
