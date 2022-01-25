/* ****************************************************************************
 * AUTHOR: Devid Dokash (780131).
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: client functions implementation. Functions for simulating a
 * client.
 * ***************************************************************************/
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
)

const (
	// If true, enables depuration logging.
	kEnableDebugLogs = true
	// If true, logging output to stdout, if not, to kLogOutputDir file.
	kLogToStdout = true
	// Logging file name.
	kLogOutputDir = "./"
)

// Returns new raft logger.
func NewLogger(name string) *log.Logger {
	if kEnableDebugLogs {
		logName := name
		logPrefix := fmt.Sprintf("%s ", logName)
		if kLogToStdout {
			return log.New(os.Stdout, logName,
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			return log.New(logOutputFile, logPrefix,
				log.Lmicroseconds|log.Lshortfile)
		}
	} else {
		return log.New(ioutil.Discard, "", 0)
	}
}

func generate_operation() raft.Operation {
	rand.Seed(time.Now().UnixNano())
	op := raft.Operation{}
	op.Name = "READ"
	op.Key = strconv.Itoa(rand.Intn(10))
	if rand.Intn(100) > 50 {
		op.Name = "WRITE"
		op.Value = strconv.Itoa(rand.Intn(1000))
	}
	return op
}

func find_leader(peers []rpctimeout.HostPort) rpctimeout.HostPort {
	reply := &raft.GetStateReply{}
	for {
		for _, peer := range peers {
			peer.CallTimeout("RaftNode.GetState", &raft.Any{}, reply,
				10*time.Millisecond)
			if reply.Leader {
				return peer
			}
		}
	}
}

func main() {
	logger := NewLogger("cltraft")
	logger.Println("hi, im a disinterested client come from nowhere for your " +
		"raft implementation as kind soul")
	peers_str := os.Args[2:]
	peers := rpctimeout.StringArrayToHostPortArray(peers_str)
	leader := find_leader(peers)

	for {
		args := generate_operation()
		reply := &raft.SubmitOperationReply{}
		logger.Println("operation to submit:", args)
		err := leader.CallTimeout("RaftNode.SubmitOperation", args, reply,
			500*time.Millisecond)
		if err != nil {
			logger.Println("SubmitOperation RPC Call error with", leader, "-",
				err)
		} else {
			logger.Println("operation submitted, received, gotten value:",
				reply.Value, "index:", reply.Index, ", term:", reply.Term,
				", still leader?", reply.Leader, " who's leader?",
				reply.LeaderId)
			if !reply.Leader {
				leader = find_leader(peers)
			}
		}
		time.Sleep(500)
	}
}
