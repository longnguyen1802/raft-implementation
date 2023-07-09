package raft

import (
	"encoding/json"
	"io/ioutil"
)

type Snapshot struct {
	LastIncludeIndex int      `json:"lastIncludeIndex"`
	LastIncludeTerm  int      `json:"lastIncludeTerm"`
	Logs             []string `json:"log"`
}

func SaveSnapshot(snapshot Snapshot, filename string) error {
	snapShotdata, err := json.Marshal(snapshot)
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
	return snapshot, err
}
