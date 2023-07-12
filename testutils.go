package raft

import (
	"fmt"
	"reflect"
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
	idCM1 := cm1.id
	lastIncludedIndexCM1 := cm1.lastIncludedIndex
	lastIncludedTermCM1 := cm1.lastIncludedTerm
	currentTermCM1 := cm1.currentTerm
	logCM1 := cm1.log
	commitIndexCM1 := cm1.commitIndex
	cm1.mu.Unlock()
	cm2.mu.Lock()
	idCM2 := cm2.id
	lastIncludedIndexCM2 := cm2.lastIncludedIndex
	lastIncludedTermCM2 := cm2.lastIncludedTerm
	currentTermCM2 := cm2.currentTerm
	logCM2 := cm2.log
	commitIndexCM2 := cm2.commitIndex
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
	for {
		cm1snapshot, _ := GetSnapshot(getSnapshotFile(idCM1, lastIncludedIndexCM1/SNAPSHOT_LOGSIZE))
		cm2snapshot, _ := GetSnapshot(getSnapshotFile(idCM2, lastIncludedIndexCM2/SNAPSHOT_LOGSIZE))
		if !cm1snapshot.compare(&cm2snapshot) {
			return false
		}
		lastIncludedIndexCM1 -= SNAPSHOT_LOGSIZE
		lastIncludedIndexCM2 -= SNAPSHOT_LOGSIZE
		if lastIncludedIndexCM1 == 0 && lastIncludedIndexCM2 == 0 {
			return true
		}
	}
}

func getSnapshotFile(serverId int, snapshotIndex int) string {
	return fmt.Sprintf("snapshot/server%d/%d.json", serverId, snapshotIndex)
}
