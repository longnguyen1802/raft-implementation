package debug

//var debuglock sync.Mutex

// func DebugLog(serverId int, format string, args ...interface{}) {
// 	debuglock.Lock()
// 	defer debuglock.Unlock()
// 	f, err := os.OpenFile("debuglog/server"+strconv.Itoa(serverId)+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
// 	if err != nil {
// 		log.Fatalf("error opening file: %v", err)
// 	}
// 	defer f.Close()

// 	log.SetOutput(f)
// 	format = fmt.Sprintf("[%d] ", serverId) + format
// 	log.Printf(format, args...)
// }
