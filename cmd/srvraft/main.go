/* ****************************************************************************
 * AUTHOR: Devid Dokash (780131)
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: Raft node Server main.
 * ***************************************************************************/
package main

import (
	//"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"

	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
)

// Main function for implement the storage service through Raft consensous
// algorythm.
//  main <id> <hostports...>
func main() {
	// Server configuration.
	me, _ := strconv.Atoi(os.Args[1])
	peers_str := os.Args[2:]
	peers := rpctimeout.StringArrayToHostPortArray(peers_str)
	cce := make(chan raft.CommitEntry, 1000)
	nr := raft.New(peers, me, cce)
	rpc.Register(nr)

	l, err := net.Listen("tcp", peers_str[me])
	check.CheckError(err, "Main listen error:")

	// State machine and storage configuration.
	storage := make(map[string]string)
	go func() {
		for commit := range cce {
			nr.Logger.Println("Main - new commit: ", commit.CommitResult)
			if commit.CommitResult != nil {
				if value, ok := storage[commit.Op.Key]; ok {
					commit.CommitResult <- value
				} else {
					commit.CommitResult <- ""
				}
			}
			if commit.Op.Name == "WRITE" {
				if commit.Op.Value != "" {
					storage[commit.Op.Key] = commit.Op.Value
				} else {
					delete(storage, commit.Op.Key)
				}
			}
		}
	}()

	// Periodic logging of storage. Comment if not needed.
	go func() {
		timeOut := time.NewTicker(1 * time.Second)
		for range timeOut.C {
			nr.Logger.Println("Main - current state of storage: ", storage)
		}
	}()

	rpc.Accept(l)
}
