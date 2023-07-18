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

// Do not check for PeerClients
func (c *Configuration) Compare(other *Configuration) bool {
	if c.ClusterSize != other.ClusterSize {
		return false
	}
	return reflect.DeepEqual(c.MachineInfo, other.MachineInfo)
}
