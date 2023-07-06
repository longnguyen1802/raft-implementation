package raft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
	//"github.com/fortytw2/leaktest"
)

// It is similar to a crash where server cannot send request and response to any other server
func IsolatedServer(servers []*Server, id int, num_server int) {
	servers[id].DisconnectAll()
	for i := 0; i < num_server; i++ {
		if id != i {
			servers[i].DisconnectPeer(id)
		}
	}
}
func RestoreIsolatedServer(servers []*Server, id int, num_server int) {
	for i := 0; i < num_server; i++ {
		if id != i {
			servers[i].ConnectToPeer(id, servers[id].GetListenAddr())
			servers[id].ConnectToPeer(i, servers[i].GetListenAddr())
		}
	}
}

// func TestElectionBasic(t *testing.T) {
// 	num_server := 3
// 	servers := make([]*Server, num_server)
// 	connected := make([]bool, num_server)
// 	ready := make(chan interface{})
// 	for i := 0; i < num_server; i++ {
// 		peerIds := make([]int, 0)
// 		for p := 0; p < num_server; p++ {
// 			if p != i {
// 				peerIds = append(peerIds, p)
// 			}
// 		}

// 		servers[i] = NewServer(i, peerIds, ready)
// 		servers[i].Serve()
// 	}

// 	for i := 0; i < num_server; i++ {
// 		for j := 0; j < num_server; j++ {
// 			if i != j {
// 				servers[i].ConnectToPeer(j, servers[j].GetListenAddr())
// 			}
// 		}
// 		connected[i] = true
// 	}

// 	client := NewClient(servers)
// 	close(ready)

// 	go func() {
// 		time.Sleep(1 * time.Second)
// 		for j:=0;j<60;j++{
// 			for i := 0; i < num_server; i++ {
// 				result := client.sendCommand(i, "command "+strconv.Itoa(rand.Intn(1000)))
// 				if result {
// 					break
// 				}
// 			}
// 		}
// 		// for {
// 		// 	time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)
// 		// 	for j:=0;j<1+rand.Intn(3);j++{
// 		// 		for i := 0; i < num_server; i++ {
// 		// 			result := client.sendCommand(i, "command "+strconv.Itoa(rand.Intn(1000)))
// 		// 			if result {
// 		// 				break
// 		// 			}
// 		// 		}
// 		// 	}

// 		// }

//		}()
//		time.Sleep(6 * time.Second)
//	}
func TestElectionBasic(t *testing.T) {
	num_server := 5
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
		start := time.Now()
		crash_event := true
		time.Sleep(1 * time.Second)
		rand_machine_1 := rand.Intn(num_server)
		rand_machine_2 := 0
		for {
			rand_machine_2 = rand.Intn(num_server)
			if rand_machine_2 != rand_machine_1 {
				break
			}

		}
		rand_machine_3 := 0
		for {
			rand_machine_3 = rand.Intn(num_server)
			if rand_machine_3 != rand_machine_2 && rand_machine_3 != rand_machine_1 {
				break
			}

		}
		restore_machine_1 := true
		restore_machine_2 := true
		//restore_machine_3 := true
		for {
			elapsed := time.Now()
			if elapsed.Sub(start) > 3*time.Second && crash_event {
				crash_event = false
				IsolatedServer(servers, rand_machine_1, num_server)
				IsolatedServer(servers, rand_machine_2, num_server)
				IsolatedServer(servers, rand_machine_3, num_server)
			}
			if elapsed.Sub(start) > 5*time.Second && restore_machine_1 {
				restore_machine_1 = false
				RestoreIsolatedServer(servers, rand_machine_1, num_server)
				IsolatedServer(servers, rand_machine_2, num_server)
				IsolatedServer(servers, rand_machine_3, num_server)
			}
			if elapsed.Sub(start) > 7*time.Second && restore_machine_2 {
				restore_machine_2 = false
				RestoreIsolatedServer(servers, rand_machine_2, num_server)
				IsolatedServer(servers, rand_machine_3, num_server)
			}
			time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)
			for j := 0; j < 1+rand.Intn(3); j++ {
				for _, i := range rand.Perm(num_server) {
					result := client.sendCommand(i, "command "+strconv.Itoa(rand.Intn(1000)))
					if result {
						break
					}
				}
			}
		}

	}()
	time.Sleep(9 * time.Second)
}
