package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
)

const SNAPSHOT_LOGSIZE = 32

type Snapshot struct {
	LastIncludedIndex int   `json:"lastIncludedIndex"`
	LastIncludedTerm  int   `json:"lastIncludedTerm"`
	Logs              []Log `json:"log"`
}

func (s *Snapshot) Compare(other *Snapshot) bool {
	if s.LastIncludedIndex != other.LastIncludedIndex || s.LastIncludedTerm != other.LastIncludedTerm {
		return false
	}
	return reflect.DeepEqual(s.Logs, other.Logs)
}

type StableStorage struct {
	serverId int
}

func NewStableStorage(serverId int) *StableStorage {
	ss := new(StableStorage)
	ss.serverId = serverId
	return ss
}

func (ss *StableStorage) GetTerm(index int) int {
	fileIndex := index/SNAPSHOT_LOGSIZE + 1
	snapshot := ss.GetSnapshot(fileIndex)
	return snapshot.Logs[index%SNAPSHOT_LOGSIZE].Term
}

func (ss *StableStorage) GetLogSlice(from int, to int) []Log {
	begin := from
	logSlice := make([]Log, to-from)
	for {
		snapshot := ss.GetSnapshot(from/SNAPSHOT_LOGSIZE + 1)

		if from/SNAPSHOT_LOGSIZE == to/SNAPSHOT_LOGSIZE {
			copy(logSlice[from-begin:to-begin], snapshot.Logs[from%SNAPSHOT_LOGSIZE:to%SNAPSHOT_LOGSIZE])
			break
		} else {
			copy(logSlice[from-begin:(from/SNAPSHOT_LOGSIZE+1)*SNAPSHOT_LOGSIZE-begin], snapshot.Logs[from%SNAPSHOT_LOGSIZE:])
			from = (from/SNAPSHOT_LOGSIZE + 1) * SNAPSHOT_LOGSIZE
			if from == to {
				break
			}
		}
	}
	return logSlice
}

func (ss *StableStorage) GetSnapshot(fileIndex int) Snapshot {

	snapshot, _ := GetSnapshotFromFile(ss.GetFileName(fileIndex))
	// if err != nil {
	// 	//debug.DebugLog(ss.serverId, "Error when get snapshot from file %v", ss.GetFileName(fileIndex))
	// }
	return snapshot
}

func (ss *StableStorage) GetFileName(fileIndex int) string {
	return fmt.Sprintf("snapshot/server%d/%d.json", ss.serverId, fileIndex)
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
