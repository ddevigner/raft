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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"

	"github.com/google/go-cmp/cmp"
)



var peers []rpctimeout.HostPort

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

func stateToString(reply *raft.GetStateReply, ctx string, log bool) {
	fmt.Println(ctx, "my id:", reply.Me, ", current term: ", reply.Term,
		", is leader:", reply.Leader, ", leader id:", reply.LeaderId)
	if log {
		fmt.Println("\tcurrent log: ", reply.Log)
	}
}

func operationToString(op raft.Operation, ctx string) {
	fmt.Println(ctx, "operation:", op.Name, ", key:", op.Key, ", value:",
		op.Value)
}

func submitResToString(reply *raft.SubmitOperationReply, ctx string) {
	fmt.Println(ctx, ", gotten value:", reply.Value, ", index:", reply.Index,
		", term:", reply.Term, ", still leader?", reply.Leader,
		", who's leader?", reply.LeaderId)
}

func GetLeader(leader *raft.GetStateReply) {
	stateToString(leader, "last leader -", false)
	reply := &raft.GetStateReply{}
	for {
		for _, peer := range peers {
			peer.CallTimeout("RaftNode.GetState", &raft.GetStateArgs{}, reply,
				10*time.Millisecond)
			if reply.Leader {
				stateToString(reply, "new leader --", false)
				*leader = *reply
				return
			}
		}
		fmt.Println("not leader found, trying again.")
	}
}

func GetState(peer rpctimeout.HostPort, log bool) {
	reply := &raft.GetStateReply{}
	err := peer.CallTimeout("RaftNode.GetState",
		&raft.GetStateArgs{SendLog: log}, reply, 10*time.Millisecond)
	if err != nil {
		fmt.Println(peer, "unreachable.")
		return
	}
	stateToString(reply, string(peer)+" state -", log)
}

func submitOperation(i int, leader *raft.GetStateReply, concurrent bool) {
	mtx := sync.Mutex{}
	for {
		args := generate_operation()
		reply := &raft.SubmitOperationReply{}
		operationToString(args, strconv.Itoa(i+1)+"th operation to submit -")
		check.CheckError(peers[leader.Me].CallTimeout(
			"RaftNode.SubmitOperation", args, reply, 500*time.Millisecond),
			"client submit operation error -")
		if reply.Leader {
			submitResToString(reply, "operation submitted -")
			return
		} else {
			if concurrent {
				mtx.Lock()
				GetLeader(leader)
				mtx.Unlock()
			} else {
				GetLeader(leader)
			}
		}
	}
}

func SubmitOperations(leader *raft.GetStateReply, n int, concurrent bool) {
	if concurrent {
		for i := 0; i < n; i++ {
			go submitOperation(i, leader, concurrent)
			time.Sleep(500)
		}
	} else {
		for i := 0; i < n; i++ {
			submitOperation(i, leader, concurrent)
			time.Sleep(500)
		}
	}
}

func CheckLogs(leader *raft.GetStateReply) {
	args := &raft.GetStateArgs{SendLog: true}
	for {
		reply := &raft.GetStateReply{}
		peers[leader.Me].CallTimeout("RaftNode.GetState", args, reply,
			10*time.Millisecond)
		if reply.Leader {
			stateToString(reply, "reference", true)
			other_peers := append(
				append([]rpctimeout.HostPort{}, peers[:leader.Me]...),
				peers[leader.Me+1:]...)
			for _, v := range other_peers {
				next_reply := &raft.GetStateReply{}
				err := v.CallTimeout("RaftNode.GetState", args, next_reply,
					10*time.Millisecond)
				if err == nil {
					if reply.Term != next_reply.Term ||
						reply.LeaderId != next_reply.LeaderId ||
						!cmp.Equal(reply.Log, next_reply.Log) {
						fmt.Print("different states?")
					}
					stateToString(next_reply, "obtained: ", true)
				}
			}
			return
		} else {
			GetLeader(leader)
		}
	}
}

func main() {
	fmt.Println("hi, im a disinterested client come from nowhere for your " +
		"raft implementation as kind soul")
	peers = rpctimeout.StringArrayToHostPortArray(os.Args[1:])
	leader := &raft.GetStateReply{}
	GetLeader(leader)
	option := 0
	other_option := 0
	for {
		fmt.Println("\n(0) Get the current leader.", "\n(1) Get node state",
			"\n(2) Submit operations.", "\n(3) Check the logs.\n(4) Exit.")
		switch fmt.Scan(&option); option {
		case 0:
			fmt.Println("-- Getting the leader state.")
			GetLeader(leader)
		case 1:
			fmt.Println("-- Which node do you want to get state from? [1-",
				len(peers), "]")
			fmt.Scan(&option)
			switch {
			case option > 3:
				option = 3
			case option < 1:
				option = 1
			}
			fmt.Println("-- With or without log? (0) Yes (1) No")
			fmt.Scan(&other_option)
			if other_option == 0 {
				GetState(peers[option-1], true)
			} else {
				GetState(peers[option-1], false)
			}
		case 2:
			fmt.Println("-- How many operations?")
			fmt.Scan(&option)
			fmt.Println("-- Concurrent? (0) Yes, (1) No")
			fmt.Scan(&other_option)
			if other_option == 0 {
				SubmitOperations(leader, option, true)
			} else {
				SubmitOperations(leader, option, false)
			}
		case 3:
			fmt.Println("-- Checking logs.")
			CheckLogs(leader)
		case 4:
			fmt.Println("-- Bye bye.")
			os.Exit(0)
		}
	}
}
