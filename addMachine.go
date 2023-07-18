package raft

import (
	"net"
	"reflect"
)

type MachineInformation struct {
	Protocol string
	Address  string
}

type Configuration struct {
	ClusterSize int
	MachineIds  []int
	MachineInfo map[int]MachineInformation
}

func NewConfiguration(size int, ids []int) *Configuration {
	c := new(Configuration)
	c.ClusterSize = size
	c.MachineIds = make([]int, size)
	copy(c.MachineIds, ids)
	c.MachineInfo = make(map[int]MachineInformation)
	return c
}

func (c *Configuration) updateMachine(id int, addr net.Addr) {
	c.MachineInfo[id] = MachineInformation{
		Protocol: addr.Network(),
		Address:  addr.String(),
	}
}

func (c *Configuration) addMachine(id int, info MachineInformation) {
	c.ClusterSize += 1
	c.MachineIds = append(c.MachineIds, id)
	c.MachineInfo[id] = info
}

// Do not check for PeerClients
func (c *Configuration) Compare(other *Configuration) bool {
	if c.ClusterSize != other.ClusterSize {
		return false
	}
	return reflect.DeepEqual(c.MachineInfo, other.MachineInfo)
}

type AddMachineArgs struct {
	Id   int
	Info MachineInformation
}

type AddMachineResponse struct {
	Status bool
}

func (cm *ConsensusModule) AddMachine(args AddMachineArgs, response *AddMachineResponse) error {
	cm.mu.Lock()
	if cm.state != Leader {
		response.Status = false
		return nil
	}
	// Change config first
	cm.config.addMachine(args.Id, args.Info)
	// Update all index of that machine
	cm.nextIndex[args.Id] = cm.getLogSize()
	cm.matchIndex[args.Id] = -1
	cm.matchIncludedIndex[args.Id] = 0
	cm.mu.Unlock()
	cm.SubmitCommand("add machine")
	response.Status = true
	return nil
}

func (cm *ConsensusModule) sendAddMachine() {
	cm.mu.Lock()

	peerIds := cm.peerIds
	cm.mu.Unlock()
	for _, peerId := range peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			args := AddMachineArgs{
				Id: cm.id,
				Info: MachineInformation{
					Protocol: cm.server.GetListenAddr().Network(),
					Address:  cm.server.GetListenAddr().String(),
				},
			}
			var response AddMachineResponse
			cm_server := cm.server
			cm.debugLog("Argument to send addmachine is %+v", args)
			cm.mu.Unlock()
			if err := cm_server.Call(peerId, "ConsensusModule.AddMachine", args, &response); err != nil {
				cm.mu.Lock()
				cm.debugLog("Send Add machine RPC success")
				cm.mu.Unlock()
			} else {
				cm.mu.Lock()
				cm.debugLog("Error when send Add machine RPC", err)
				cm.mu.Unlock()
			}
		}(peerId)
	}
}
