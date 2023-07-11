package raft

import (
	"testing"
	//"github.com/fortytw2/leaktest"
)

func TestUtilServices(t *testing.T) {
	cm := ConsensusModule {
		id: 1,
		lastIncludedIndex: 32,
		log: []Log{
			{
				Term: 16,
				Command: "Test 0",
			},
			{
				Term: 16,
				Command: "Test 1",
			},
			{
				Term: 16,
				Command: "Test 2",
			},
		},
	}
	// t.Logf("Log size is %d",cm.getLogSize())
	// t.Logf("Index for term 672 is %d",cm.getTerm(672))
	// t.Logf("Slice from 30 to 66 is %+v",cm.getLogSlice(30,66))
	term,slice :=cm.getTermAndSliceForIndex(32)
	t.Logf("Get term and slice for 32 Term: %d and Slice: %+v",term,slice)
}