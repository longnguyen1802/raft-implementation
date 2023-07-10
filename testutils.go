package raft

// It is similar to a crash where server cannot send request and response to any other server
func DisconectTwoServer(servers []*Server, id1 int, id2 int) {
	servers[id1].DisconnectPeer(id2)
	servers[id2].DisconnectPeer(id1)
}
func ConnectTwoServer(servers []*Server, id1 int, id2 int) {
	servers[id1].ConnectToPeer(id2, servers[id2].GetListenAddr())
	servers[id2].ConnectToPeer(id1, servers[id1].GetListenAddr())
}
func IsolatedServer(servers []*Server, id int, num_server int) {
	for i := 0; i < num_server; i++ {
		if id != i {
			DisconectTwoServer(servers, i, id)
		}
	}
}
func RestoreIsolatedServer(servers []*Server, id int, num_server int, failures []bool) {
	for i := 0; i < num_server; i++ {
		if id != i && !failures[i]  {
			ConnectTwoServer(servers, id, i)
		}
	}
}

func SetupServerTesting(servers []*Server, num_server int, ready chan interface{}) {
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
	}
}
