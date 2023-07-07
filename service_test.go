package raft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
	//"github.com/fortytw2/leaktest"
)

// It is similar to a crash where server cannot send request and response to any other server
func DisconectTwoServer(servers []*Server,id1 int,id2 int){
	servers[id1].DisconnectPeer(id2)
	servers[id2].DisconnectPeer(id1)
}
func ConnectTwoServer(servers []*Server,id1 int,id2 int){
	servers[id1].ConnectToPeer(id2, servers[id2].GetListenAddr())
	servers[id2].ConnectToPeer(id1, servers[id1].GetListenAddr())
}
func IsolatedServer(servers []*Server, id int, num_server int) {
	for i := 0; i < num_server; i++ {
		if id != i {
			DisconectTwoServer(servers,i,id)
		}
	}
}
func RestoreIsolatedServer(servers []*Server, id int, num_server int,failures []bool) {
	for i := 0; i < num_server; i++ {
		if id != i && (failures[i]==false){
			ConnectTwoServer(servers,id,i)
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
	rand.Seed(time.Now().UnixNano())
	num_server := 5
	servers := make([]*Server, num_server)
	failures := make([]bool,num_server)
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
		failures[i] = false
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
		t.Logf("Failure machine is %d %d %d",rand_machine_1,rand_machine_2,rand_machine_3)
		restore_machine_1 := true
		restore_machine_2 := true
		//restore_machine_3 := true
		for {
			elapsed := time.Now()
			if crash_event && elapsed.Sub(start) > 3*time.Second{
				crash_event = false
				failures[rand_machine_1]= true
				failures[rand_machine_2]= true
				failures[rand_machine_3]= true
				IsolatedServer(servers, rand_machine_1, num_server)
				IsolatedServer(servers, rand_machine_2, num_server)
				IsolatedServer(servers, rand_machine_3, num_server)
			}
			if elapsed.Sub(start) > 5*time.Second && restore_machine_1 {
				restore_machine_1 = false
				t.Logf("Restore machine %d",rand_machine_1)
				RestoreIsolatedServer(servers, rand_machine_1, num_server,failures)
				failures[rand_machine_1] = false
			}
			if elapsed.Sub(start) > 7*time.Second && restore_machine_2 {
				restore_machine_2 = false
				t.Logf("Restore machine %d",rand_machine_2)
				RestoreIsolatedServer(servers, rand_machine_2, num_server,failures)
				failures[rand_machine_2] = false
			}
			if elapsed.Sub(start) < 11*time.Second {
				time.Sleep(time.Duration(50+rand.Intn(50)) * time.Millisecond)
				for j := 0; j < 1+rand.Intn(4); j++ {
					for _, i := range rand.Perm(num_server) {
						result := client.sendCommand(i, "command "+strconv.Itoa(rand.Intn(1000)))
						if result {
							break
						}
					}
				}
			}
		}

	}()
	time.Sleep(15 * time.Second)
}


// type Set struct {
// 	myset map[int]struct{}
// }

// func (s *Set) Add(x int) {
// 	s.myset[x] = struct{}{}
// }

// func (s *Set) Remove(x int) {
// 	delete(s.myset, x)
// }

// func (s *Set) Exist(x int) bool {
// 	if _, ok := s.myset[x]; ok {
// 		return true
// 	} else {
// 		return false
// 	}
// }
// func (s *Set) Print() {
// 	fmt.Println(s.myset)
// }