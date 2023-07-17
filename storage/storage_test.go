package storage

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func GenerateRandomSnapshot(index int) Snapshot {
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

func TestStorageSaveSnapshot(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	err := SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))
	if err != nil {
		t.Errorf("error is %v", err)
	}
	if !fileExists(si.stable.GetFileName(1)) {
		t.Errorf("Error when save snapshot to file")
	}
	os.Remove(si.stable.GetFileName(1))
}

func TestStorageGetSnapshot(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	err := SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))
	if err != nil {
		t.Errorf("error is %v", err)
	}
	if !fileExists(si.stable.GetFileName(1)) {
		t.Errorf("Error when save snapshot to file")
	}

	snapshot_get, err := GetSnapshotFromFile(si.stable.GetFileName(1))
	if err != nil {
		t.Errorf("error is %v", err)
	}
	if !snapshot1.Compare(&snapshot_get) {
		t.Errorf("Got different between save snapshot and get snapshot from file")
	}
	os.Remove(si.stable.GetFileName(1))
}

func TestStorageGetTermStable(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	err := SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))
	if err != nil {
		t.Errorf("error is %v", err)
	}
	if !fileExists(si.stable.GetFileName(1)) {
		t.Errorf("Error when save snapshot to file")
	}

	random_num := rand.Intn(SNAPSHOT_LOGSIZE)

	term := si.GetTerm(random_num)

	if term != snapshot1.Logs[random_num].Term {
		t.Errorf("Error when get term from stable storage of index %d, expected %d got %d", random_num, snapshot1.Logs[random_num].Term, term)
	}
	os.Remove(si.stable.GetFileName(1))
}

func TestStorageGetTermVolatile(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	err := SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))
	if err != nil {
		t.Errorf("error is %v", err)
	}
	if !fileExists(si.stable.GetFileName(1)) {
		t.Errorf("Error when save snapshot to file")
	}

	index := SNAPSHOT_LOGSIZE

	term := si.GetTerm(index)

	if term != si.volatile.log[0].Term {
		t.Errorf("Error when get term from stable storage of index %d, expected %d got %d", index, si.volatile.log[0].Term, term)
	}
	os.Remove(si.stable.GetFileName(1))
}

func TestStorageGetLogSliceStable(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))

	logSlice := si.GetLogSlice(12, 32)
	expectedLogSlice := snapshot1.Logs[12:32]
	if !reflect.DeepEqual(logSlice, expectedLogSlice) {
		t.Errorf("Error when get slice from stable expected %+v, got %+v", expectedLogSlice, logSlice)
	}
}
func TestStorageGetLogSliceStableTwo(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE * 3,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))
	snapshot2 := GenerateRandomSnapshot(2)
	SaveSnapshotToFile(snapshot2, si.stable.GetFileName(2))
	snapshot3 := GenerateRandomSnapshot(3)
	SaveSnapshotToFile(snapshot3, si.stable.GetFileName(3))

	logSlice := si.GetLogSlice(12, 35)
	expectedLogSlice := snapshot1.Logs[12:SNAPSHOT_LOGSIZE]
	expectedLogSlice = append(expectedLogSlice, snapshot2.Logs[0:35-SNAPSHOT_LOGSIZE]...)
	if !reflect.DeepEqual(logSlice, expectedLogSlice) {
		t.Errorf("Error when get slice from stable expected %+v, got %+v", expectedLogSlice, logSlice)
	}
	logSlice = si.GetLogSlice(11, 69)
	expectedLogSlice = snapshot1.Logs[11:32]
	expectedLogSlice = append(expectedLogSlice, snapshot2.Logs[0:SNAPSHOT_LOGSIZE]...)
	expectedLogSlice = append(expectedLogSlice, snapshot3.Logs[0:69-2*SNAPSHOT_LOGSIZE]...)
	if !reflect.DeepEqual(logSlice, expectedLogSlice) {
		t.Errorf("Error when get slice from stable expected \n %+v, got \n %+v", expectedLogSlice, logSlice)
	}
	os.Remove(si.stable.GetFileName(1))
	os.Remove(si.stable.GetFileName(2))
	os.Remove(si.stable.GetFileName(3))
}
func TestStorageGetLogSliceStableMix(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE * 3,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	snapshot1 := GenerateRandomSnapshot(1)
	SaveSnapshotToFile(snapshot1, si.stable.GetFileName(1))
	snapshot2 := GenerateRandomSnapshot(2)
	SaveSnapshotToFile(snapshot2, si.stable.GetFileName(2))
	snapshot3 := GenerateRandomSnapshot(3)
	SaveSnapshotToFile(snapshot3, si.stable.GetFileName(3))

	logSlice := si.GetLogSlice(39, 98)
	expectedLogSlice := snapshot2.Logs[39-SNAPSHOT_LOGSIZE : SNAPSHOT_LOGSIZE]
	expectedLogSlice = append(expectedLogSlice, snapshot3.Logs[0:SNAPSHOT_LOGSIZE]...)
	expectedLogSlice = append(expectedLogSlice, si.volatile.log[0], si.volatile.log[1])
	if !reflect.DeepEqual(logSlice, expectedLogSlice) {
		t.Errorf("Error when get slice from stable expected \n%+v, got \n%+v", expectedLogSlice, logSlice)
	}

	os.Remove(si.stable.GetFileName(1))
	os.Remove(si.stable.GetFileName(2))
	os.Remove(si.stable.GetFileName(3))
}

func TestStorageAppendCommand(t *testing.T) {
	si := StorageInterface{
		volatile: &VolatileStorage{
			lastIncludedIndex: SNAPSHOT_LOGSIZE,
			log: []Log{
				{
					Term:    16,
					Command: "Test 0",
				},
				{
					Term:    16,
					Command: "Test 1",
				},
				{
					Term:    16,
					Command: "Test 2",
				},
			},
		},
		stable: &StableStorage{
			serverId: 100,
		},
	}
	si.AppendCommand("Test 3", 17)
	if si.volatile.log[3].Term != 17 || si.volatile.log[3].Command != "Test 3" {
		t.Errorf("Error when append new command to the log")
	}
}
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
