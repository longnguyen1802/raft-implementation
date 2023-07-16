package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

const SNAPSHOT_LOGSIZE = 32

type Snapshot struct {
	LastIncludedIndex int   `json:"lastIncludedIndex"`
	LastIncludedTerm  int   `json:"lastIncludedTerm"`
	Logs              []Log `json:"log"`
}

type StableStorage struct{
	serverId int
}

func (ss *StableStorage) GetTerm(index int) int{
	fileIndex := index/SNAPSHOT_LOGSIZE +1
	snapshot,_ := ss.GetSnapshot(fileIndex)
	return snapshot.Logs[index].Term
}

func (ss *StableStorage) GetLogSlice(from int,to int) []Log {
	//
	return nil
}

func (ss*StableStorage) GetSnapshot(fileIndex int) (Snapshot,error){
	return GetSnapshotFromFile(ss.getFileName(fileIndex))
}

func (ss *StableStorage) getFileName(fileIndex int) string {
	return fmt.Sprintf("snapshot/server%d/%d.json",ss.serverId,fileIndex)
}


func makeDirIfNotExist(dirPath string) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		// Create the directory
		err := os.Mkdir(dirPath, 0755)
		if err != nil {
			log.Fatal("Failed to create directory:", err)
		}
	}
}
func SaveSnapshotToFile(snapshot Snapshot, filename string) error {
	snapShotdata, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, snapShotdata, 0644)
	return err
}

func GetSnapshotFromFile(filename string) (Snapshot, error) {
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