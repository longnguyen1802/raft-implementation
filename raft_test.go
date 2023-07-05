package raft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
	//"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
	num_server := 3
	servers := make([]*Server, num_server)
	connected := make([]bool, num_server)
	ready := make(chan interface{})
	for i := 0; i < num_server; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < num_server; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		servers[i] = NewServer(i, peerIds, ready)
		servers[i].Serve()
	}

	for i := 0; i < num_server; i++ {
		for j := 0; j < num_server; j++ {
			if i != j {
				servers[i].ConnectToPeer(j, servers[j].GetListenAddr())
			}
		}
		connected[i] = true
	}

	client := NewClient(servers)
	close(ready)

	go func() {
		time.Sleep(1 * time.Second)
		for j:=0;j<60;j++{
			for i := 0; i < num_server; i++ {
				result := client.sendCommand(i, "command "+strconv.Itoa(rand.Intn(1000)))
				if result {
					break
				}
			}
		}
		// for {
		// 	time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)
		// 	for j:=0;j<1+rand.Intn(3);j++{
		// 		for i := 0; i < num_server; i++ {
		// 			result := client.sendCommand(i, "command "+strconv.Itoa(rand.Intn(1000)))
		// 			if result {
		// 				break
		// 			}
		// 		}
		// 	}
			
		// }

	}()
	time.Sleep(6 * time.Second)
}
