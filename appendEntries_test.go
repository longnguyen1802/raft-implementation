package raft

import (
	"testing"
	"reflect"
)

func TestAppendEntries(t *testing.T) {
	tests := []struct {
		name string
		cm *ConsensusModule
		args AppendEntriesArgs
		want AppendEntriesResponse
		checkLog bool
		wantLog []Log
	} {
		{
			name: "Test startup append entries",
			cm: &ConsensusModule{
				id:100,
				currentTerm: 1,
				votedFor: -1,
				applyStateMachineEvent: make(chan struct{}, 16),
			},
			args: AppendEntriesArgs {
				Term: 1,
				LeaderId: 1,
				PrevLogIndex: -1,
				PrevLogTerm: -1,
				Entries: []Log{
					{
						Command: "command",
						Term: 1,
					},
				},
				LeaderCommit: 0,
			},
			want: AppendEntriesResponse {
				Term: 1,
				Success: true,
			},
			checkLog: true,
			wantLog: []Log{
				{
					Command: "command",
					Term: 1,
				},
			},
		},
		{
			name: "Test appendEntries leader outdate",
			cm: &ConsensusModule{
				id:100,
				currentTerm: 5,
				applyStateMachineEvent: make(chan struct{}, 16),
			},
			args: AppendEntriesArgs {
				Term: 1,
				LeaderId: 1,
				PrevLogIndex: -1,
				PrevLogTerm: -1,
				Entries: []Log{
					{
						Command: "command",
						Term: 1,
					},
				},
				LeaderCommit: -1,
			},
			want: AppendEntriesResponse {
				Term: 5,
				Success: false,
			},
			checkLog: false,
			wantLog: []Log{},
		},
		{
			name: "Test appendEntries log missing latest entries",
			cm: &ConsensusModule{
				id:100,
				currentTerm: 1,
				applyStateMachineEvent: make(chan struct{}, 16),
			},
			args: AppendEntriesArgs {
				Term: 1,
				LeaderId: 1,
				PrevLogIndex: 2,
				PrevLogTerm: 1,
				Entries: []Log{
					{
						Command: "command",
						Term: 1,
					},
				},
				LeaderCommit: 1,
			},
			want: AppendEntriesResponse {
				Term: 1,
				Success: false,
			},
			checkLog: false,
			wantLog: []Log{},
		},
		{
			name: "Test appendEntries remove existing entries",
			cm: &ConsensusModule{
				id:100,
				currentTerm: 2,
				applyStateMachineEvent: make(chan struct{}, 16),
				commitIndex: 2,
				log: []Log{
					{
						Command: "command",
						Term: 1,
					},
					{
						Command: "command",
						Term: 2,
					},
					{
						Command: "command",
						Term: 3,
					},
					{
						Command: "command",
						Term: 4,
					},
					{
						Command: "command",
						Term: 5,
					},
				},
			},
			args: AppendEntriesArgs {
				Term: 6,
				LeaderId: 1,
				PrevLogIndex: 2,
				PrevLogTerm: 3,
				Entries: []Log{
					{
						Command: "command 1",
						Term: 6,
					},
					{
						Command: "command 2",
						Term: 6,
					},
				},
				LeaderCommit: 4,
			},
			want: AppendEntriesResponse {
				Term: 6,
				Success: true,
			},
			checkLog: true,
			wantLog: []Log{
				{
					Command: "command",
					Term: 1,
				},
				{
					Command: "command",
					Term: 2,
				},
				{
					Command: "command",
					Term: 3,
				},
				{
					Command: "command 1",
					Term: 6,
				},
				{
					Command: "command 2",
					Term: 6,
				},
			},
		},
	}
	for _,tt := range tests {
		t.Run(tt.name, func(t *testing.T){
			// Simulate a running statemachine
			go tt.cm.applyStateMachine()
			response := &AppendEntriesResponse{}
			err := tt.cm.AppendEntries(tt.args,response)
			if err!= nil{
				t.Errorf("AppendEntries returned an error: %v", err)
			}

			if *response != tt.want {
				t.Errorf("AppendEntries response doesn't match expected value. Got %+v, want %+v", *response, tt.want)
			}
			if tt.checkLog {
				if !reflect.DeepEqual(tt.cm.log,tt.wantLog) {
					t.Errorf("AppendEntries log response doesn't match expected value. Got %+v, want %+v", tt.cm.log, tt.wantLog)
				}
			}
			// Check sync with leader commit if AppendEntries RPC return true
			if tt.want.Success {
				if tt.args.LeaderCommit != tt.cm.commitIndex {
					t.Errorf("AppendEntries commit index response doesn't match expected value. Got %+v, want %+v",tt.cm.commitIndex,tt.args.LeaderCommit)
				}
			}
		})
	}
}
