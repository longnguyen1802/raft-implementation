package raft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
	//"github.com/fortytw2/leaktest"
)

func TestRunNormal(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	num_server := 5
	servers := make([]*Server, num_server)
	failures := make([]bool, num_server)
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
	}

	client := NewClient(servers)
	close(ready)

	go func() {
		start := time.Now()
		time.Sleep(1 * time.Second)
		for {
			elapsed := time.Now()
			if elapsed.Sub(start) < 9*time.Second {
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
	time.Sleep(12 * time.Second)
}
func TestServices(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	num_server := 5
	servers := make([]*Server, num_server)
	failures := make([]bool, num_server)
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
		t.Logf("Failure machine is %d %d %d", rand_machine_1, rand_machine_2, rand_machine_3)
		restore_machine_1 := true
		restore_machine_2 := true
		restore_machine_3 := true
		for {
			elapsed := time.Now()
			if crash_event && elapsed.Sub(start) > 3*time.Second {
				crash_event = false
				failures[rand_machine_1] = true
				failures[rand_machine_2] = true
				failures[rand_machine_3] = true
				IsolatedServer(servers, rand_machine_1, num_server)
				IsolatedServer(servers, rand_machine_2, num_server)
				IsolatedServer(servers, rand_machine_3, num_server)
			}
			if elapsed.Sub(start) > 10*time.Second && restore_machine_1 {
				restore_machine_1 = false
				t.Logf("Restore machine %d", rand_machine_1)
				RestoreIsolatedServer(servers, rand_machine_1, num_server, failures)
				failures[rand_machine_1] = false
			}
			if elapsed.Sub(start) > 20*time.Second && restore_machine_2 {
				restore_machine_2 = false
				t.Logf("Restore machine %d", rand_machine_2)
				RestoreIsolatedServer(servers, rand_machine_2, num_server, failures)
				failures[rand_machine_2] = false
			}
			if elapsed.Sub(start) < 30*time.Second {
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
			if elapsed.Sub(start) > 40*time.Second && restore_machine_3 {
				restore_machine_3 = false
				t.Logf("Restore machine %d", rand_machine_3)
				RestoreIsolatedServer(servers, rand_machine_3, num_server, failures)
				failures[rand_machine_3] = false
			}
		}

	}()
	time.Sleep(60 * time.Second)
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
