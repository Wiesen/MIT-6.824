package raft

import "log"

// Debugging
const Debug = 0

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}
