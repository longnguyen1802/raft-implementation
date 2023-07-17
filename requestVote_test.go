package raft

// import (
// 	"testing"
// 	//"time"
// )

// func TestRequestVote(t *testing.T) {
// 	tests := []struct {
// 		name string
// 		cm   *ConsensusModule
// 		args RequestVoteArgs
// 		want RequestVoteResponse
// 	}{
// 		{
// 			name: "Test startup request vote",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 1,
// 				votedFor:    -1,
// 			},
// 			args: RequestVoteArgs{
// 				Term:         1,
// 				CandidateId:  101,
// 				LastLogIndex: -1,
// 				LastLogTerm:  -1,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        1,
// 				VoteGranted: true,
// 			},
// 		},
// 		{
// 			name: "Test outdated request vote",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 2,
// 				votedFor:    -1,
// 			},
// 			args: RequestVoteArgs{
// 				Term:         1,
// 				CandidateId:  101,
// 				LastLogIndex: -1,
// 				LastLogTerm:  -1,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        2,
// 				VoteGranted: false,
// 			},
// 		},
// 		{
// 			name: "Test request server already vote",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 1,
// 				votedFor:    2,
// 			},
// 			args: RequestVoteArgs{
// 				Term:         1,
// 				CandidateId:  101,
// 				LastLogIndex: -1,
// 				LastLogTerm:  -1,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        1,
// 				VoteGranted: false,
// 			},
// 		},
// 		{
// 			name: "Test request vote for old server",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 1,
// 				votedFor:    101,
// 			},
// 			args: RequestVoteArgs{
// 				Term:         1,
// 				CandidateId:  101,
// 				LastLogIndex: -1,
// 				LastLogTerm:  -1,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        1,
// 				VoteGranted: true,
// 			},
// 		},
// 		{
// 			name: "Test log up-to-date vote",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 1,
// 				votedFor:    -1,
// 				log: []Log{
// 					{
// 						Command: "command",
// 						Term:    1,
// 					},
// 					{
// 						Command: "command",
// 						Term:    2,
// 					},
// 					{
// 						Command: "command",
// 						Term:    3,
// 					},
// 					{
// 						Command: "command",
// 						Term:    4,
// 					},
// 					{
// 						Command: "command",
// 						Term:    5,
// 					},
// 				},
// 			},
// 			args: RequestVoteArgs{
// 				Term:         5,
// 				CandidateId:  101,
// 				LastLogIndex: 6,
// 				LastLogTerm:  5,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        5,
// 				VoteGranted: true,
// 			},
// 		},
// 		{
// 			name: "Test log term not up-to-date vote",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 1,
// 				votedFor:    -1,
// 				log: []Log{
// 					{
// 						Command: "command",
// 						Term:    1,
// 					},
// 					{
// 						Command: "command",
// 						Term:    2,
// 					},
// 					{
// 						Command: "command",
// 						Term:    3,
// 					},
// 					{
// 						Command: "command",
// 						Term:    4,
// 					},
// 					{
// 						Command: "command",
// 						Term:    5,
// 					},
// 				},
// 			},
// 			args: RequestVoteArgs{
// 				Term:         5,
// 				CandidateId:  101,
// 				LastLogIndex: 3,
// 				LastLogTerm:  5,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        5,
// 				VoteGranted: false,
// 			},
// 		},
// 		{
// 			name: "Test term up to date but not log term not up-to-date vote",
// 			cm: &ConsensusModule{
// 				id:          100,
// 				currentTerm: 1,
// 				votedFor:    -1,
// 				log: []Log{
// 					{
// 						Command: "command",
// 						Term:    1,
// 					},
// 					{
// 						Command: "command",
// 						Term:    2,
// 					},
// 					{
// 						Command: "command",
// 						Term:    3,
// 					},
// 					{
// 						Command: "command",
// 						Term:    4,
// 					},
// 					{
// 						Command: "command",
// 						Term:    5,
// 					},
// 				},
// 			},
// 			args: RequestVoteArgs{
// 				Term:         7,
// 				CandidateId:  101,
// 				LastLogIndex: 3,
// 				LastLogTerm:  5,
// 			},
// 			want: RequestVoteResponse{
// 				Term:        7,
// 				VoteGranted: false,
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			response := &RequestVoteResponse{}
// 			err := tt.cm.RequestVote(tt.args, response)
// 			if err != nil {
// 				t.Errorf("RequestVote returned an error: %v", err)
// 			}

// 			if *response != tt.want {
// 				t.Errorf("RequestVote response doesn't match expected value. Got %+v, want %+v", *response, tt.want)
// 			}
// 		})
// 	}
// }
