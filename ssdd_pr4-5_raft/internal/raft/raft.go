/* ****************************************************************************
 * AUTHOR: Devid Dokash (780131)
 * SUBJECT: Distributed Systems.
 * DATE: January/2022
 * DESCRIPTION: full Raft node implementation for applying a system based on
 * the Raft Consensus Algorythm.
 * ***************************************************************************/
package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"raft/internal/comun/rpctimeout"
)

/* ============================================================================
 * RAFT NODE Logger
 * ========================================================================= */

const (
	// If true, enables depuration logging.
	kEnableDebugLogs = true
	// If true, logging output to stdout, if not, to kLogOutputDir file.
	kLogToStdout = false
	// Logging file name.
	kLogOutputDir = "./logs_raft/"
)

// Returns new raft Logger.
func NewLogger(me rpctimeout.HostPort) *log.Logger {
	if kEnableDebugLogs {
		logName := me.Host() + "_" + me.Port() + "_"
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
				kLogOutputDir, logPrefix[:len(logPrefix)-1]),
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

/* ============================================================================
 * RAFT NODE INFO AND IMPLEMENTATION.
 * ========================================================================= */

// Raft node state type.
type State int

// Raft node state constants.
const (
	// Follower state.
	FOLLOWER State = iota
	// Candidate state.
	CANDIDATE
	// Leader state.
	LEADER
)

// Empty struct.
type Any struct{}

// Operation, conformed by the name (Read/Write), key of the storage service
// and its value ("" if read).
type Operation struct {
	Name  string // Operation name.
	Key   string // Value key.
	Value string // Value.
}

// State machine commit entry, a commit entry is conformed by an operation, its
// correspondent index in the entry log and the correspondent channel for
// returning the applied value.
type CommitEntry struct {
	Op           Operation   // Commit entry operation.
	Index        int         // Index in entries log.
	CommitResult chan string // Channel related to client request for
	// retrieving the request result.
}

// Log entry, an entry is conformed by an operation, the term when it was
// submitted and the correspondent channel for returning the applied value.
type Entry struct {
	Op           Operation   // Log entry operation.
	Term         int         // Log entry term.
	CommitResult chan string // Each channel for each new client submit.
}

// Raft node implementation.
type RaftNode struct {
	// Raft node implementation dependencies:
	mux          sync.Mutex            // Shared access mutex.
	nodes        []rpctimeout.HostPort // All raft nodes hostports.
	me           int                   // This node id.
	state        State                 // This node state.
	leader       int                   // Leader node id.
	Logger       *log.Logger           // Depuration log. One per node.
	commit_entry chan CommitEntry      // Channel for client requests to
	// apply into the machine state.
	the_end_of_state chan bool          // End state function channel.
	new_timeout      chan time.Duration // Timeout channel.

	// Persistent state on all servers:
	currentTerm int     // Node current term.
	votedFor    int     // Node current voted node id.
	log         []Entry // Operations/Entries log.

	// Volatile state on all servers:
	commitIndex int // Highest log entry to commit.
	lastApplied int // Highest log entry applied.

	// Volatile state on leaders:
	nextIndex []int // Optimistic prediction of nodes log next index to append
	// entries.
	matchIndex []int // Real nodes log next index to append entries.
}

// Returns new raft node.
func New(peers []rpctimeout.HostPort, me int, cce chan CommitEntry) *RaftNode {
	nr := &RaftNode{
		nodes: append(append([]rpctimeout.HostPort{},
			peers[:me]...), peers[me+1:]...),
		me:               me,
		state:            FOLLOWER,
		leader:           -1,
		Logger:           NewLogger(peers[me]),
		commit_entry:     cce,
		the_end_of_state: make(chan bool),
		new_timeout:      make(chan time.Duration),
		votedFor:         -1,
		log:              []Entry{},
	}
	//nr.logging(1)
	go nr.become_as_followers()
	nr.Logger.Println("Logger initialized.")
	return nr
}

// Logging function.
func (nr *RaftNode) logging(gap int) {
	timeOut := time.NewTicker(time.Duration(gap) * time.Second)
	for range timeOut.C {
		nr.Logger.Println("RAFT NODE LOG ===================")
		nr.Logger.Println("ME:             ", nr.me)
		nr.Logger.Println("STATE:          ", nr.state)
		nr.Logger.Println("LEADER:         ", nr.leader)
		nr.Logger.Println("CURRENT TERM:   ", nr.currentTerm)
		nr.Logger.Println("VOTED FOR:      ", nr.votedFor)
		nr.Logger.Println("CURRENT LOG:    ", nr.log)
		nr.Logger.Println("CURRENT COMMIT: ", nr.commitIndex)
		nr.Logger.Println("LAST APPLIED:   ", nr.lastApplied)
		nr.Logger.Println("NEXT INDEXES:   ", nr.nextIndex)
		nr.Logger.Println("MATCH INDEXES:  ", nr.matchIndex)
	}
}

// Updates the commit index.
func (nr *RaftNode) update_commitIndex() {
	for successes := -1; nr.commitIndex < len(nr.log) &&
		(successes == -1 || successes*2 >
			len(nr.nodes)+1); nr.commitIndex++ {
		if nr.log[nr.commitIndex].Term == nr.currentTerm {
			successes = 0
			for _, v := range nr.matchIndex {
				if v >= nr.commitIndex {
					successes++
				}
			}
		}
	}
}

// Updates the last applied index.
func (nr *RaftNode) update_lastApplied() {
	for ; nr.lastApplied < nr.commitIndex; nr.lastApplied++ {
		nr.commit_entry <- CommitEntry{
			nr.log[nr.lastApplied].Op,
			nr.lastApplied,
			nr.log[nr.lastApplied].CommitResult,
		}
	}
}

/* ============================================================================
 * RPC CALL: stop raft node.
 * ========================================================================= */

// RPC Method for stopping raft node execution.
func (nr *RaftNode) Stop(any *Any, reply *Any) error {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
	return nil
}

/* ============================================================================
 * RPC CALL: raft node state info.
 * ========================================================================= */

// Request from getting the node log.
type GetStateArgs struct {
	SendLog bool
}

// Raft node state, conformed by its own id, the current term, if its leader
// and the leader id.
type GetStateReply struct {
	Me       int
	Term     int
	Leader   bool
	LeaderId int
	Log      []Entry
}

// RPC Method for returning raft node state, its own id, current term and if
// its leader and the leader id. It can also return the node log, if SendLog of
// GetStateArgs is true, it will return the entries between MinId and MaxId as
// possible.
func (nr *RaftNode) GetState(args *GetStateArgs, reply *GetStateReply) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()

	reply.Me = nr.me
	reply.Term = nr.currentTerm
	if nr.state == LEADER {
		reply.Leader = true
	}
	reply.LeaderId = nr.leader
	if args.SendLog {
		reply.Log = nr.log
	}

	return nil
}

/* ============================================================================
 * RPC CALL: submit operation.
 * ========================================================================= */

// State of the submitted operation, conformed by the result value obtained
// from applying it in the state machine, its log index position and the term
// when it was submitted, if the node is leader and the id of the system
// leader.
type SubmitOperationReply struct {
	Value    string
	Index    int
	Term     int
	Leader   bool
	LeaderId int
}

// RPC Method for submitting operations and waits until they are applied in the
// state machine returning the correspondent result value, only if the node
// is leader.
func (nr *RaftNode) SubmitOperation(op Operation,
	reply *SubmitOperationReply) error {
	nr.mux.Lock()
	reply.LeaderId = nr.leader
	if nr.state == LEADER {
		result := make(chan string)
		nr.log = append(nr.log, Entry{op, nr.currentTerm, result})

		reply.Index = len(nr.log) + 1
		reply.Term = nr.currentTerm
		reply.Leader = true

		nr.mux.Unlock()

		reply.Value = <-result
		close(result)
	} else {
		nr.mux.Unlock()
	}

	return nil
}

/* ============================================================================
 * MAIN ALGORYTHM RAFT FUNCTIONALITY.
 * ========================================================================= */

// The node becomes a follower.
func (nr *RaftNode) become_as_followers() {
	rand.Seed(time.Now().UnixNano())
	election_timeout := time.Duration(rand.Intn(150) + 150)
	for {
		select {
		case election_timeout = <-nr.new_timeout:
		case <-time.After(election_timeout * time.Millisecond):
			go nr.change_state(CANDIDATE, "INITIATING NEW ELECTIONS")
			return
		}
	}
}

// The node becomes a candidate.
func (nr *RaftNode) become_as_candidates() {
	rand.Seed(time.Now().UnixNano())
	election_timeout := time.Duration(rand.Intn(150) + 150)
	elections := 1
	nr.elections()
	for {
		select {
		case <-nr.the_end_of_state:
			return
		case <-time.After(election_timeout * time.Millisecond):
			nr.mux.Lock()
			if nr.state == CANDIDATE {
				elections++
				nr.mux.Unlock()
				nr.Logger.Println("Candidate - initiating the next elections.",
					" Currently have", elections, "election(s)")
				nr.mux.Lock()
				nr.currentTerm++
				nr.mux.Unlock()
				rand.Seed(time.Now().UnixNano())
				election_timeout = time.Duration(rand.Intn(150) + 150)
				nr.elections()
			} else {
				nr.mux.Unlock()
				return
			}
		}
	}
}

// The node becomes a leader.
func (nr *RaftNode) become_as_leaders() {
	nr.heartbeat_replicant()
	for {
		select {
		case <-nr.the_end_of_state:
			return
		case <-time.After(50 * time.Millisecond):
			nr.mux.Lock()
			if nr.state == LEADER {
				nr.mux.Unlock()
				nr.Logger.Println("Leader - new heartbeat for my subdits.")
				nr.heartbeat_replicant()
			} else {
				nr.mux.Unlock()
				return
			}
		}
	}
}

// Changes the node current state.
func (nr *RaftNode) change_state(state State, why_changing string) {
	nr.mux.Lock()
	defer nr.mux.Unlock()

	switch nr.state {
	case FOLLOWER:
		nr.Logger.Println("Follower -", why_changing)
	case CANDIDATE:
		nr.Logger.Println("Candidate -", why_changing)
		nr.the_end_of_state <- true
	case LEADER:
		nr.Logger.Println("Leader -", why_changing)
		nr.the_end_of_state <- true
	}

	switch nr.state = state; state {
	case FOLLOWER:
		nr.Logger.Println(" === SPAWNING AS FOLLOWER  === ")
		nr.votedFor = -1
		go nr.become_as_followers()
	case CANDIDATE:
		nr.Logger.Println(" === SPAWNING AS CANDIDATE === ")
		nr.currentTerm++
		nr.votedFor = nr.me
		go nr.become_as_candidates()
	case LEADER:
		nr.Logger.Println(" === SPAWNING  AS  LEADER  === ")
		nr.leader = nr.me
		nr.nextIndex = []int{}
		nr.matchIndex = []int{}
		for range nr.nodes {
			nr.nextIndex = append(nr.nextIndex, len(nr.log)+1)
			nr.matchIndex = append(nr.matchIndex, -1)
		}
		go nr.become_as_leaders()
	}
}

/* ============================================================================
 * RPC CALL: appending entries.
 * ========================================================================= */

// Request message for appending new entries or heartbeat.
type AppendEntriesArgs struct {
	LeaderTerm   int     // Leader term.
	LeaderId     int     // Leader id.
	PrevLogIndex int     // Last leader log entry index.
	PrevLogTerm  int     // Term of leader log[PrevLogIndex]
	Entries      []Entry // Leader log entries to replicate.
	LeaderCommit int     // Leader commit index.
}

// Reply message for forthcoming entries or heartbeat.
type AppendEntriesReply struct {
	NodeTerm int  // Node reply sender current term.
	Success  bool // Replicating log success.
}

// RPC Method for appending new entries or heartbeat.
func (nr *RaftNode) AppendEntries(args *AppendEntriesArgs,
	reply *AppendEntriesReply) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()

	switch nr.state {
	case FOLLOWER:
		nr.Logger.Println("Follower - new leader append entries:", *args)
	case CANDIDATE:
		nr.Logger.Println("Candidate - new leader append entries:", *args)
	case LEADER:
		nr.Logger.Println("Leader - new leader append entries:", *args)
	}

	reply.NodeTerm = nr.currentTerm
	if nr.currentTerm <= args.LeaderTerm {
		if nr.state == FOLLOWER {
			nr.new_timeout <- time.Duration(rand.Intn(150) + 150)
		} else {
			go nr.change_state(FOLLOWER, "outdated term via append entries.")
		}
		nr.leader = args.LeaderId
		nr.currentTerm = args.LeaderTerm
		if args.PrevLogIndex == -1 {
			reply.Success = true
			nr.log = args.Entries
		} else {
			if len(nr.log) > args.PrevLogIndex &&
				nr.log[args.PrevLogIndex].Term == args.PrevLogTerm {
				reply.Success = true
				nr.log = append(nr.log[:args.PrevLogIndex+1], args.Entries...)
				if nr.commitIndex < args.LeaderCommit {
					nr.commitIndex = func() int {
						if len(nr.log)-1 > args.LeaderCommit {
							return args.LeaderCommit
						} else {
							return len(nr.log) - 1
						}
					}()
					for ; nr.lastApplied < nr.commitIndex; nr.lastApplied++ {
						nr.commit_entry <- CommitEntry{
							nr.log[nr.lastApplied].Op,
							nr.lastApplied,
							nil,
						}
					}
				}
			}
		}
	}
	return nil
}

// Produces heartbeats, both as empty and with entries, towards every node.
func (nr *RaftNode) heartbeat_replicant() {
	for i, v := range nr.nodes {
		go func(index int, hp rpctimeout.HostPort) {
			nr.mux.Lock()
			prevLogIndex := nr.nextIndex[index] - 2
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = nr.log[prevLogIndex].Term
			}
			entries := nr.log[nr.nextIndex[index]-1:]
			args := &AppendEntriesArgs{
				LeaderTerm:   nr.currentTerm,
				LeaderId:     nr.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: nr.commitIndex,
			}
			reply := &AppendEntriesReply{}
			nr.mux.Unlock()

			if err := hp.CallTimeout(
				"RaftNode.AppendEntries",
				args,
				reply,
				time.Duration(10*time.Millisecond),
			); err == nil {
				nr.mux.Lock()
				defer nr.mux.Unlock()

				if reply.NodeTerm > nr.currentTerm {
					go nr.change_state(FOLLOWER, "outdated term via heartbeat")
					return
				}
				if nr.state == LEADER && nr.currentTerm == reply.NodeTerm {
					if reply.Success {
						nr.nextIndex[index] += len(args.Entries)
						nr.matchIndex[index] = nr.nextIndex[index] - 1
						lastCommitIndex := nr.commitIndex
						nr.update_commitIndex()
						if lastCommitIndex != nr.commitIndex {
							nr.update_lastApplied()
						}
					} else {
						nr.nextIndex[index]--
					}
				}
			}
		}(i, v)
	}
}

/* ============================================================================
 * RPC CALL: requesting votes.
 * ========================================================================= */

// Request message for a new vote request.
type RequestVoteArgs struct {
	CandidateTerm         int // Candidate's term.
	CandidateId           int // Candidate's id.
	CandidateLastLogIndex int // Candidate's last log entry index.
	CandidateLastLogTerm  int // Candidate's last log entry term.
}

// Reply message for a forthcoming vote request.
type RequestVoteReply struct {
	VoterTerm   int  // Current node voter term.
	VoteGranted bool // Granted vote or not to the requesting Candidate.
}

// RPC Method for requesting votes.
func (nr *RaftNode) RequestVote(args *RequestVoteArgs,
	vote *RequestVoteReply) error {
	nr.mux.Lock()
	defer nr.mux.Unlock()

	switch nr.state {
	case FOLLOWER:
		nr.Logger.Println("Follower - candidate requesting my vote:", *args)
	case CANDIDATE:
		nr.Logger.Println("Candidate - candidate requesting my vote:", *args)
	case LEADER:
		nr.Logger.Println("Leader - candidate requesting my vote:", *args)
	}

	vote.VoterTerm = nr.currentTerm
	if nr.currentTerm < args.CandidateTerm {
		nr.currentTerm = args.CandidateTerm
		lastLogIndex := len(nr.log) - 1
		if nr.state == FOLLOWER {
			nr.new_timeout <- time.Duration(rand.Intn(150) + 150)
		} else {
			go nr.change_state(FOLLOWER, "outdated term through request vote.")
			return nil
		}
		if (nr.votedFor == -1 || nr.votedFor == args.CandidateId) ||
			(lastLogIndex == -1) ||
			((nr.log[lastLogIndex].Term < args.CandidateLastLogTerm) ||
				((nr.log[lastLogIndex].Term == args.CandidateLastLogTerm) &&
					lastLogIndex <= args.CandidateLastLogIndex)) {
			vote.VoteGranted = true
			nr.votedFor = args.CandidateId
		}
	}
	return nil
}

// Starts new elections.
func (nr *RaftNode) elections() {
	nr.mux.Lock()
	elections_term := nr.currentTerm
	nr.mux.Unlock()

	votes_in_favor := 1
	//elections_mux  := &sync.Mutex{}
	for _, v := range nr.nodes {
		go func(hp rpctimeout.HostPort) {
			nr.mux.Lock()
			lastLogIndex := len(nr.log) - 1
			candidateLastLogTerm := -1
			if lastLogIndex > 0 {
				candidateLastLogTerm = nr.log[lastLogIndex].Term
			}
			args := &RequestVoteArgs{
				CandidateTerm:         nr.currentTerm,
				CandidateId:           nr.me,
				CandidateLastLogIndex: lastLogIndex,
				CandidateLastLogTerm:  candidateLastLogTerm,
			}
			reply := &RequestVoteReply{}
			nr.mux.Unlock()

			if err := hp.CallTimeout("RaftNode.RequestVote",
				args,
				reply,
				time.Duration(10*time.Millisecond),
			); err == nil {
				nr.mux.Lock()
				defer nr.mux.Unlock()
				if nr.state == CANDIDATE && elections_term == nr.currentTerm {
					if reply.VoteGranted {
						votes_in_favor++
					}
					if votes_in_favor*2 > len(nr.nodes)+1 {
						go nr.change_state(LEADER, "elected as leader")
					}
				}
			}
		}(v)
	}
}
