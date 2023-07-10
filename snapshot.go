package raft

import (
	"encoding/json"
	"io/ioutil"
)

type Snapshot struct {
	LastIncludedIndex int   `json:"lastIncludedIndex"`
	LastIncludedTerm  int   `json:"lastIncludedTerm"`
	Logs              []Log `json:"log"`
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
