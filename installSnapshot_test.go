package raft

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
	"reflect"
)

func getFollowerAndLeaderId() (int, int) {
	followerId := 100
	leaderId := 101
	return followerId, leaderId
}
func generateRandomSnapshot(index int) Snapshot {
	rand.Seed(time.Now().UnixNano())
	term := rand.Intn(100) + 1
	logs := make([]Log, SNAPSHOT_LOGSIZE)
	for i := 0; i < SNAPSHOT_LOGSIZE; i++ {
		logs[i] = Log{
			Term:    term,
			Command: "command " + strconv.Itoa(rand.Intn(1000)),
		}
	}

	return Snapshot{
		LastIncludedIndex: SNAPSHOT_LOGSIZE * index,
		LastIncludedTerm:  term,
		Logs:              logs,
	}
}

func TestInstallSnapshot(t *testing.T) {
	followerId, leaderId := getFollowerAndLeaderId()

	tests := []struct {
		name          string
		cm            *ConsensusModule
		args          InstallSnapshotArgs
		want          InstallSnapshotResponse
		checkSnapshot bool
	}{
		{
			name: "Test normal install snapshot update lastIncludedIndex",
			cm: &ConsensusModule{
				id:                     followerId,
				currentTerm:            2,
				votedFor:               -1,
				applyStateMachineEvent: make(chan struct{}, 16),
				lastIncludedIndex:      SNAPSHOT_LOGSIZE,
			},
			args: InstallSnapshotArgs{
				Term:              2,
				LeaderId:          leaderId,
				LastIncludedIndex: 3*SNAPSHOT_LOGSIZE,
				LastIncludedTerm:  123,
				Offset:            0,
				Data:              Snapshot{},
			},
			want: InstallSnapshotResponse{
				Term:              2,
				LastIncludedIndex: SNAPSHOT_LOGSIZE,
			},
			checkSnapshot: false,
		},
		{
			name: "Test send empty Log",
			cm: &ConsensusModule{
				id:                     followerId,
				currentTerm:            2,
				votedFor:               -1,
				applyStateMachineEvent: make(chan struct{}, 16),
				lastIncludedIndex:      2*SNAPSHOT_LOGSIZE,
			},
			args: InstallSnapshotArgs{
				Term:              2,
				LeaderId:          leaderId,
				LastIncludedIndex: 2*SNAPSHOT_LOGSIZE,
				LastIncludedTerm:  123,
				Offset:            0,
				Data:              Snapshot{},
			},
			want: InstallSnapshotResponse{
				Term:              2,
				LastIncludedIndex: 2*SNAPSHOT_LOGSIZE,
			},
			checkSnapshot: false,
		},
		{
			name: "Test follower update LastIncludedIndex to leader",
			cm: &ConsensusModule{
				id:                     followerId,
				currentTerm:            2,
				votedFor:               -1,
				applyStateMachineEvent: make(chan struct{}, 16),
				lastIncludedIndex:      4*SNAPSHOT_LOGSIZE,
			},
			args: InstallSnapshotArgs{
				Term:              2,
				LeaderId:          leaderId,
				LastIncludedIndex: 2*SNAPSHOT_LOGSIZE,
				LastIncludedTerm:  123,
				Offset:            0,
				Data:              Snapshot{},
			},
			want: InstallSnapshotResponse{
				Term:              2,
				LastIncludedIndex: 4*SNAPSHOT_LOGSIZE,
			},
			checkSnapshot: false,
		},
		{
			name: "Test do take install snapshot from leader",
			cm: &ConsensusModule{
				id:                     followerId,
				currentTerm:            2,
				votedFor:               -1,
				applyStateMachineEvent: make(chan struct{}, 16),
				lastIncludedIndex:      2*SNAPSHOT_LOGSIZE,
				commitIndex:			SNAPSHOT_LOGSIZE-1,
			},
			args: InstallSnapshotArgs{
				Term:              2,
				LeaderId:          leaderId,
				LastIncludedIndex: 3*SNAPSHOT_LOGSIZE,
				LastIncludedTerm:  123,
				Offset:            2*SNAPSHOT_LOGSIZE,
				Data:              generateRandomSnapshot(3),
			},
			want: InstallSnapshotResponse{
				Term:              2,
				LastIncludedIndex: 3*SNAPSHOT_LOGSIZE,
			},
			checkSnapshot: true,
		},
		
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate a running statemachine
			go tt.cm.applyStateMachine()
			response := &InstallSnapshotResponse{}
			err := tt.cm.InstallSnapshot(tt.args, response)
			if err != nil {
				t.Errorf("Installsnapshot returned an error: %v", err)
			}

			if *response != tt.want {
				t.Errorf("Installsnapshot response doesn't match expected value. Got %+v, want %+v", *response, tt.want)
			}
			if tt.checkSnapshot {
				getSnapshot,_ := GetSnapshot(getSnapshotFile(followerId,tt.args.LastIncludedIndex/SNAPSHOT_LOGSIZE))
				if !reflect.DeepEqual(tt.args.Data.Logs, getSnapshot.Logs) {
					t.Errorf("Installsnapshot doesn't match expected value. Got %+v, want %+v", getSnapshot.Logs, tt.args.Data.Logs)
				}
			}
		})
	}
	// For apply state machine to run (avoid any error)
	time.Sleep(1*time.Second)
	cleanSnapshot(100)
}

func cleanSnapshot(serverId int) {
	folder := fmt.Sprintf("snapshot/server%d", serverId)
	os.RemoveAll(folder)
}

func getSnapshotFile(serverId int, snapshotIndex int) string {
	return fmt.Sprintf("snapshot/server%d/%d.json", serverId, snapshotIndex)
}
