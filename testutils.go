package raft

import (
	"reflect"

	"github.com/nhatlong/raft/storage"
)

// It is similar to a crash where server cannot send request and response to any other server
func DisconectTwoServer(servers []*Server, id1 int, id2 int) {
	servers[id1].DisconnectPeer(id2)
	servers[id2].DisconnectPeer(id1)
}
func ConnectTwoServer(servers []*Server, id1 int, id2 int) {
	servers[id1].ConnectToPeer(id2, servers[id2].GetListenAddr())
	servers[id2].ConnectToPeer(id1, servers[id1].GetListenAddr())
}
func IsolatedServer(servers []*Server, id int, num_server int) {
	for i := 0; i < num_server; i++ {
		if id != i {
			DisconectTwoServer(servers, i, id)
		}
	}
}
func RestoreIsolatedServer(servers []*Server, id int, num_server int, failures []bool) {
	for i := 0; i < num_server; i++ {
		if id != i && !failures[i] {
			ConnectTwoServer(servers, id, i)
		}
	}
}

func SetupServerTesting(servers []*Server, num_server int, ready chan interface{}) {
	for i := 0; i < num_server; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < num_server; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		servers[i] = NewServer(i, peerIds, ready)
		servers[i].Serve()
	}

	for i := 0; i < num_server; i++ {
		for j := 0; j < num_server; j++ {
			if i != j {
				servers[i].ConnectToPeer(j, servers[j].GetListenAddr())
			}
		}
	}
}

func compareConsensusState(cm1 *ConsensusModule, cm2 *ConsensusModule) bool {
	cm1.mu.Lock()
	lastIncludedIndexCM1 := cm1.GetLastIncludedIndex()
	lastIncludedTermCM1 := cm1.GetLastIncludedTerm()
	currentTermCM1 := cm1.GetCurrentTerm()
	logCM1 := cm1.getLog()
	commitIndexCM1 := cm1.GetCommitIndex()
	cm1.mu.Unlock()
	cm2.mu.Lock()
	lastIncludedIndexCM2 := cm2.GetLastIncludedIndex()
	lastIncludedTermCM2 := cm2.GetLastIncludedTerm()
	currentTermCM2 := cm2.GetCurrentTerm()
	logCM2 := cm2.getLog()
	commitIndexCM2 := cm2.GetCommitIndex()
	cm2.mu.Unlock()
	// Compare ram state
	if lastIncludedIndexCM1 != lastIncludedIndexCM2 || lastIncludedTermCM1 != lastIncludedTermCM2 {
		return false
	}
	if currentTermCM1 != currentTermCM2 || commitIndexCM1 != commitIndexCM2 {
		return false
	}
	if !reflect.DeepEqual(logCM1, logCM2) {
		return false
	}
	// Compare snapshot state
	cm1.mu.Lock()
	cm2.mu.Lock()
	defer cm1.mu.Lock()
	defer cm2.mu.Lock()
	for {
		cm1snapshot := cm1.GetSnapshot(lastIncludedIndexCM1 / storage.SNAPSHOT_LOGSIZE)
		//storage.GetSnapshot(getSnapshotFile(idCM1, lastIncludedIndexCM1/SNAPSHOT_LOGSIZE))
		cm2snapshot := cm2.GetSnapshot(lastIncludedIndexCM2 / storage.SNAPSHOT_LOGSIZE)
		//storage.(getSnapshotFile(idCM2, lastIncludedIndexCM2/SNAPSHOT_LOGSIZE))
		if !cm1snapshot.Compare(&cm2snapshot) {
			return false
		}
		lastIncludedIndexCM1 -= storage.SNAPSHOT_LOGSIZE
		lastIncludedIndexCM2 -= storage.SNAPSHOT_LOGSIZE
		if lastIncludedIndexCM1 == 0 && lastIncludedIndexCM2 == 0 {
			return true
		}
	}
}
