/* ****************************************************************************
 * AUTHOR: Devid Dokash (780131) y Victor Manuel Marcuello (741278).
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: Raft integrations tests for checking its correct behaviour.
 * ***************************************************************************/
package tests

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"

	"github.com/google/go-cmp/cmp"
)

const (
	// 1st Machine Host.
	REPLICANT_HOST_1 = "155.210.154.195"
	// 2st Machine Host.
	REPLICANT_HOST_2 = "155.210.154.196"
	// 3rd Machine Host.
	REPLICANT_HOST_3 = "155.210.154.197"

	// Our ports: 29550-29559
	// 1st Machine Port.
	REPLICANT_PORT_1 = "29550"
	// 2nd Machine Port.
	REPLICANT_PORT_2 = "29551"
	// 3rd Machine Port.
	REPLICANT_PORT_3 = "29552"

	// 1st Replicant Hostport.
	REPLICANT_1 = REPLICANT_HOST_1 + ":" + REPLICANT_PORT_1
	// 2st Replicant Hostport.
	REPLICANT_2 = REPLICANT_HOST_2 + ":" + REPLICANT_PORT_2
	// 3st Replicant Hostport.
	REPLICANT_3 = REPLICANT_HOST_3 + ":" + REPLICANT_PORT_3

	// Main file name to run.
	//REPLICANT_MAIN = "cmd/srvraft/main.go"
	REPLICANT_MAIN = "cmd/srvraft/main"

	// Private key file name.
	PRIVKEYFILE = "id_rsa"

	// If true, displays execution progress.
	verbose = true
)

// Path of the raft server executables.
var PATH string = filepath.Join(os.Getenv("HOME"), "ingi", "ssdd", "raft")

// Full command to execute the raft servers remotely:
//  cd $HOME/ingi/ssdd/raft; /usr/local/go/bin/go run cmd/srvraft/main.go
// var REPLICANT_CMD string = "cd " + PATH + "; /usr/local/go/bin/go run " + REPLICANT_MAIN
var REPLICANT_BUILD string = "cd " + PATH + "/cmd/srvraft ; /usr/local/go/bin/go build main.go"
var REPLICANT_CMD string = "cd " + PATH + " ; " + REPLICANT_MAIN

// First range tests.
func TestRaftWithoutFails(t *testing.T) {
	// Creating raft server configuration.
	cfg := newConfig([]string{REPLICANT_1, REPLICANT_2, REPLICANT_3},
		[]bool{false, false, false})
	//defer cfg.stop()

	cfg.buildServer()

	// 1st test: starts and stops every node.
	t.Run("1ST TEST START & STOP",
		func(t *testing.T) { cfg.startAndStop(t) })

	// 2nd test: starts every node, tests if there is any leader and stops them.
	t.Run("2ND TEST FIRST LEADER ELECTION",
		func(t *testing.T) { cfg.firstLeaderElection(t) })

	// 3rd test: starts every node, tests if there is any leader, stops the
	// leader (simulating the leader fall), re-tests another leader and stops
	// every node.
	t.Run("3RD TEST LEADER FALL AND REELECTIONS",
		func(t *testing.T) { cfg.reelectLeaderAfterLeaderFall(t) })

	// 4th test: starts every node, tests if there is any leader, commits three
	// operations, and stops every node.
	t.Run("4TH TEST THREE COMMITED OPERATIONS",
		func(t *testing.T) { cfg.threeCommitedOperationsStably(t) })
}

// Second range tests.
func TestRaftWithFails(t *testing.T) {
	// Creating raft server configuration.
	cfg := newConfig([]string{REPLICANT_1, REPLICANT_2, REPLICANT_3},
		[]bool{false, false, false})
	//defer cfg.stop()

	// 5th test: consens is achieved even with fallen followers.
	t.Run("5TH TEST ACHIEVED CONSENS EVEN WITH FALLS",
		func(t *testing.T) { cfg.consensEvenFallenFollowers(t) })

	// 6th test: consens is not achieved because of fallen followers.
	t.Run("6TH TEST NO CONSENS BECAUSE OF FALLS",
		func(t *testing.T) { cfg.noConsensBecauseOfFalls(t) })

	// 7th test: 5 operation submit concurrently.
	t.Run("7TH TEST CONCURRENT OPERATION SUBMIT",
		func(t *testing.T) { cfg.concurrentOperationSubmit(t) })

}

// Output channel for output remote executions.
type remoteOutput chan string

// Closes the remote output channel and displays it content.
func (cr remoteOutput) stop() {
	//close(cr)
	if verbose {
		fmt.Println(time.Now(), "- printing ssh sessions output:")
		for s := range cr {
			fmt.Println(s)
		}
	}
}

// Remote deploy config for ssh sessions.
type deployConfig struct {
	connected []bool
	nodes     []rpctimeout.HostPort
	cr        remoteOutput
}

// Returns new deployment config.
func newConfig(nodes []string, connected []bool) *deployConfig {
	return &deployConfig{
		connected,
		rpctimeout.StringArrayToHostPortArray(nodes),
		make(remoteOutput, 2000),
	}
}

// Stops the system.
func (cfg *deployConfig) stop() {
	time.Sleep(50 * time.Millisecond)
	cfg.cr.stop()
}

/* ============================================================================
 * SUBTESTS
 * ========================================================================= */

// Starts and stops every node.
func (cfg *deployConfig) startAndStop(t *testing.T) {
	// t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node.")
	}
	cfg.startDistributedProcesses()

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

// Starts every node, tests if there is any leader and stops them.
func (cfg *deployConfig) firstLeaderElection(t *testing.T) {
	// t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node.")
	}
	cfg.startDistributedProcesses()

	// Getting a leader.
	if verbose {
		fmt.Println(time.Now(), "- getting an elected leader.")
	}
	cfg.testLeader(t)

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

// Starts every node, tests if there is any leader, stops the leader
// (simulating the leader fall), re-tests another leader and stops every node.
func (cfg *deployConfig) reelectLeaderAfterLeaderFall(t *testing.T) {
	// t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node.")
	}
	cfg.startDistributedProcesses()

	// Getting a leader.
	if verbose {
		fmt.Println(time.Now(), "- getting the first elected leader.")
	}
	id := cfg.testLeader(t)

	// Killing it.
	if verbose {
		fmt.Println(time.Now(), "- killing current leader in term.")
	}
	stopNode(cfg.nodes[id])
	cfg.connected[id] = false

	// Getting a new leader.
	if verbose {
		fmt.Println(time.Now(), "- geting the new elected leader.")
	}
	cfg.testLeader(t)

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

// Starts every node, tests if there is any leader, commits three operations,
// and stops every node.
func (cfg *deployConfig) threeCommitedOperationsStably(t *testing.T) {
	// t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node and getting a leader.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node and getting a leader.")
	}
	cfg.startDistributedProcesses()
	id := cfg.testLeader(t)

	// Commiting three operations.
	if verbose {
		fmt.Println(time.Now(), "- commiting three operations.")
	}
	cfg.commitOperations(cfg.nodes[id], 3, false, true)

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

// Starts every node, obtains a leader and disconnects a follower, ...
func (cfg *deployConfig) consensEvenFallenFollowers(t *testing.T) {
	//t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node.")
	}
	cfg.startDistributedProcesses()

	// Obtaining a leader and disconnecting a node.
	if verbose {
		fmt.Println(time.Now(), "- getting an elected leader and",
			"disconnecting a node.")
	}
	id := cfg.testLeader(t)
	node := (id + 1) % len(cfg.nodes)
	stopNode(cfg.nodes[node])
	cfg.connected[node] = false

	// Establishing some consens with the disconnected node.
	if verbose {
		fmt.Println(time.Now(), "- establishing some consens.")
	}
	cfg.commitOperations(cfg.nodes[id], 3, false, true)
	reply := getState(cfg.nodes[id], true)
	for i := 1; i <= 2; i++ {
		node := (id + i) % len(cfg.nodes)
		if cfg.connected[node] {
			cfg.checkState(t, cfg.nodes[node], reply, true)
		}
	}

	// Reconnecing raft node previously disconnected and establishing
	// some consens.
	if verbose {
		fmt.Println(time.Now(), "- reconnecting the disconnected node and",
			"establishing some consens againg.")
	}
	cfg.startDistributedProcesses()
	cfg.commitOperations(cfg.nodes[id], 3, false, true)
	reply = getState(cfg.nodes[id], true)
	for i := 1; i <= 2; i++ {
		node := (id + i) % len(cfg.nodes)
		cfg.checkState(t, cfg.nodes[node], reply, true)
	}

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

func (cfg *deployConfig) noConsensBecauseOfFalls(t *testing.T) {
	//t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node.")
	}
	cfg.startDistributedProcesses()

	// Obtaining a leader and disconnecting the another two nodes.
	if verbose {
		fmt.Println(time.Now(), "- getting an elected leader and disconnect",
			"the remaining nodes.")
	}
	id := cfg.testLeader(t)
	for i := 1; i <= 2; i++ {
		node := (id + i) % len(cfg.nodes)
		stopNode(cfg.nodes[node])
		cfg.connected[node] = false
	}

	// Initiating and establishing some consens with these disconnected nodes.
	if verbose {
		fmt.Println(time.Now(), "- establishing some consens.")
	}
	cfg.commitOperations(cfg.nodes[id], 3, false, false)

	// Reconnecting these nodes and establishing some consens.
	if verbose {
		fmt.Println(time.Now(), "- reconnecting the nodes and establishing",
			"again some consens.")
	}
	cfg.startDistributedProcesses()
	cfg.commitOperations(cfg.nodes[id], 3, false, true)
	reply := getState(cfg.nodes[id], true)
	for i := 1; i <= 2; i++ {
		node := (id + i) % len(cfg.nodes)
		cfg.checkState(t, cfg.nodes[node], reply, true)
	}

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

func (cfg *deployConfig) concurrentOperationSubmit(t *testing.T) {
	//t.Skip(time.Now().String() + "- SKIPPED.")

	// Starting every node.
	if verbose {
		fmt.Println(time.Now(), "- initiating every node.")
	}
	cfg.startDistributedProcesses()

	// Obtaining a leader and submitting one operation.
	if verbose {
		fmt.Println(time.Now(), "- getting an elected leader an submitting",
			"an operation.")
	}
	id := cfg.testLeader(t)
	cfg.commitOperations(cfg.nodes[id], 1, false, true)

	// Submitting five operations concurrently.
	if verbose {
		fmt.Println(time.Now(), "- submitting five operations concurrently.")
	}
	cfg.commitOperations(cfg.nodes[id], 5, true, true)

	// Checking nodes state.
	if verbose {
		fmt.Println(time.Now(), "- checking nodes state.")
	}
	reply := getState(cfg.nodes[id], true)
	for i := 1; i <= 2; i++ {
		node := (id + i) % len(cfg.nodes)
		cfg.checkState(t, cfg.nodes[node], reply, true)
	}

	// Stopping every node.
	if verbose {
		fmt.Println(time.Now(), "- test finishes, stopping every node.")
	}
	cfg.stopDistributedProcesses()
}

/* ============================================================================
 * AUXILIAR FUNCTIONS.
 * ========================================================================= */

// Checks if there is any leader, returns hostport index from the configuration
// correspondent to the connected leader.
func (cfg *deployConfig) testLeader(t *testing.T) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(1000 * time.Millisecond)
		termsLeadersMap := make(map[int][]int)
		for i, node := range cfg.nodes {
			if cfg.connected[i] {
				if reply := getState(node, false); reply.Leader {
					termsLeadersMap[reply.Term] =
						append(termsLeadersMap[reply.Term], i)
				}
			}
		}
		lastTermWithLeader := -1
		for term, leaders := range termsLeadersMap {
			if len(leaders) > 1 {
				t.Errorf("Expected: 1 leader. Got: %d in term %d",
					len(leaders), term)
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}
		if len(termsLeadersMap) != 0 {
			return termsLeadersMap[lastTermWithLeader][0]
		}
	}
	t.Errorf("Expected: leaders. Got: nothing.")
	return -1
}

// Calls RPC Method Stop to stop the node.
func stopNode(node rpctimeout.HostPort) {
	err := node.CallTimeout("RaftNode.Stop", &raft.Any{}, nil,
		10*time.Millisecond)
	check.CheckError(err, "Stop RPC Call error in "+node.Host()+":"+
		node.Port())
}

// Calls RPC Method GetState to get the node state.
func getState(node rpctimeout.HostPort, log bool) *raft.GetStateReply {
	reply := &raft.GetStateReply{}
	err := node.CallTimeout("RaftNode.GetState", &raft.GetStateArgs{log},
		reply, 20*time.Millisecond)
	check.CheckError(err, "GetState RPC Call error in "+node.Host()+":"+
		node.Port())
	return reply
}

// Checks if the node is the wanted node.
func (cfg *deployConfig) checkState(t *testing.T, node rpctimeout.HostPort,
	state *raft.GetStateReply, log bool) {
	reply := getState(node, log)
	if state.Term != reply.Term || state.LeaderId != reply.LeaderId ||
		!cmp.Equal(state.Log, reply.Log) {
		t.Fatalf("Incorrect replicant state:\n Reference: %+v, \n Obtained: "+
			"%+v\n", state, reply)
	}
}

// Build server remotely via SSH .
//  cd /home/{user}/ssdd/ingi/raft/cmd/srvraft ; go build main.go
func (cfg *deployConfig) buildServer() {
	for _, hp := range cfg.nodes {
		despliegue.ExecMultipleNodes(
			REPLICANT_BUILD,
			[]string{hp.Host()}, cfg.cr, PRIVKEYFILE)
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(5000 * time.Millisecond)
}

// Starts remotely via SSH each raft node.
//  cd /home/{user}/ssdd/ingi/raft/cmd/srvraft ;
//  go run cmd/srvraft/main.go i REPLICANT_1 REPLICANT_2 REPLICANT_3
func (cfg *deployConfig) startDistributedProcesses() {
	for i, hp := range cfg.nodes {
		if !cfg.connected[i] {
			despliegue.ExecMultipleNodes(
				REPLICANT_CMD+" "+strconv.Itoa(i)+
					rpctimeout.HostPortArrayToString(cfg.nodes),
				[]string{hp.Host()}, cfg.cr, PRIVKEYFILE)
			cfg.connected[i] = true
		}
		time.Sleep(500 * time.Millisecond)
	}
	time.Sleep(5000 * time.Millisecond)
}

// Stops every raft node.
func (cfg *deployConfig) stopDistributedProcesses() {
	for i, node := range cfg.nodes {
		if cfg.connected[i] {
			stopNode(node)
			cfg.connected[i] = false
		}
	}
}

// Generates a new operation to submit to the raft server.
func generate_operation() raft.Operation {
	rand.Seed(time.Now().UnixNano())
	op := raft.Operation{}
	op.Name = "READ"
	op.Key = strconv.Itoa(rand.Intn(2))
	if rand.Intn(100) > 50 {
		op.Name = "WRITE"
		op.Value = strconv.Itoa(rand.Intn(1000))
	}
	return op
}

func commitOperation(node rpctimeout.HostPort, chk bool) {
	args := generate_operation()
	reply := &raft.SubmitOperationReply{}
	if verbose {
		fmt.Println(time.Now(), "- operation to submit:", args)
	}
	err := node.CallTimeout("RaftNode.SubmitOperation", args, reply,
		500*time.Millisecond)
	if chk {
		check.CheckError(err, "SubmitOperation RPC Call error in "+
			node.Host()+":"+node.Port())
	}
	if verbose {
		fmt.Println(time.Now(), "- operation commited, log index:",
			reply.Index, ", term: ", reply.Term, ", still leader: ",
			reply.Leader)
	}
}

// Commits n operations to the given node.
func (cft *deployConfig) commitOperations(node rpctimeout.HostPort, n int,
	concurrent bool, chk bool) {
	for i := 0; i < n; i++ {
		if concurrent {
			go commitOperation(node, chk)
		} else {
			commitOperation(node, chk)
			time.Sleep(250 * time.Millisecond)
		}
	}
	//time.Sleep(1000 * time.Millisecond)
}
