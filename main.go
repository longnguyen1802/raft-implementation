package main

import "github.com/nhatlong/raft/logging"

var log = logging.GetInstance()

func main(){
	num_server := 3
	servers = make([]*Server,num_server)
	connected := make([]bool, num_server)
	ready := make(chan interface{})
	for i := 0; i < ; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < num_server; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		servers[i] = NewServer(i, peerIds,ready)
		servers[i].Serve()
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)
}