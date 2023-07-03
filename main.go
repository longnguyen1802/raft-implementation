package main

import "github.com/nhatlong/raft/logging"

var log = logging.GetInstance()

func main(){
	log.Info().Caller().Msg("Hello world")
}