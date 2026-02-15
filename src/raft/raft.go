package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

type Role int

const (
	FOLLOWER Role = iota
	CANDIDATE
	LEADER
)

var ROLES = [...]string{"FOLLOWER", "CANDIDATE", "LEADER"}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	role      Role
	dead      int32 // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// For 2A
	currentTerm int
	votedFor    int
	logs        []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// For leaders (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	applyCh           chan ApplyMsg
	heartbeatInterval time.Duration
	heartbeat         context.Context
	stopHeartbeat     context.CancelFunc

	electionTimeoutTicker   *time.Ticker
	electionTimeoutDuration time.Duration
	rng                     *rand.Rand

	Logger *log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logger.Printf("returning state (term: %v), (role: %v)", rf.currentTerm, ROLES[rf.role])
	return rf.currentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// Per Extended Raft paper Figure 2:
	// "If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower "
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	// Grant vote: 5.4.1
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimerLocked()
	}
}

// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.Logger.Printf("Sent vote request to node: %v for term: %v", server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.Logger.Printf("Got vote response from node: %v for term: %v, response: %v", server, args.Term, reply)
	return ok
}

func (rf *Raft) requestVotes(ctx context.Context) bool {
	rf.mu.Lock()
	majority := len(rf.peers)/2 + 1
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logs[lastLogIndex].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	votes := 1 // self vote
	voteCh := make(chan bool, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		peer := i
		go func() {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			if !ok {
				voteCh <- false
				return
			}

			VoteGranted := reply.VoteGranted

			rf.mu.Lock()
			// step down to let the most updated node win for election
			if reply.Term > rf.currentTerm {
				rf.role = FOLLOWER
			}
			// detect step-down and not let this node win even if get majority votes
			if rf.role == FOLLOWER {
				VoteGranted = false
			}
			rf.mu.Unlock()

			voteCh <- VoteGranted
		}()
	}

	responses := 1
	for {
		select {
		case voteRes := <-voteCh:
			// detect step-down
			rf.mu.Lock()
			role := rf.role
			rf.mu.Unlock()
			if role != CANDIDATE {
				return false
			}

			responses++
			if voteRes {
				votes++
			}
			if votes >= majority {
				return true
			}
			if responses == len(rf.peers) {
				return false
			}
		case <-ctx.Done():
			return false
		}
	}
}

type RequestAppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []Log
	LeaderCommitIdx int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm (5.1)
	if args.Term < rf.currentTerm {
		rf.Logger.Printf("Append entries ignored as current term is greater than args.Term (%v > %v)", rf.currentTerm, args.Term)
		reply.Success = false
		return
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Always become follower when receiving valid AppendEntries from current or higher term
	rf.role = FOLLOWER

	rf.resetElectionTimerLocked()

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// 3 & 4. If existing entry conflicts (same index, different term), truncate and append new entries (5.3)
	insertAt := args.PrevLogIndex + 1
	for j := range args.Entries {
		idx := insertAt + j
		if idx < len(rf.logs) {
			if rf.logs[idx].Term != args.Entries[j].Term {
				rf.logs = rf.logs[:idx]
				rf.logs = append(rf.logs, args.Entries[j:]...)
				break
			}
		} else {
			rf.logs = append(rf.logs, args.Entries[j:]...)
			break
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	lastNewIndex := len(rf.logs) - 1
	if args.LeaderCommitIdx > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIdx, lastNewIndex)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		server := i
		go func() {
			rf.mu.Lock()
			if rf.role != LEADER {
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 {
				prevLogTerm = rf.logs[prevLogIndex].Term
			}
			entries := make([]Log, len(rf.logs)-rf.nextIndex[server])
			copy(entries, rf.logs[rf.nextIndex[server]:])
			args := RequestAppendEntriesArgs{
				Term:            rf.currentTerm,
				LeaderId:        rf.me,
				PrevLogIndex:    prevLogIndex,
				PrevLogTerm:     prevLogTerm,
				Entries:         entries,
				LeaderCommitIdx: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := RequestAppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok || rf.role != LEADER || args.Term != rf.currentTerm {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.votedFor = -1
				return
			}
			if reply.Success {
				// Update nextIndex and matchIndex for follower (5.3)
				newNext := prevLogIndex + 1 + len(entries)
				if newNext > rf.nextIndex[server] {
					rf.nextIndex[server] = newNext
					rf.matchIndex[server] = newNext - 1
				}
				// If there exists N such that N > commitIndex, majority matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N (5.3, 5.4)
				for n := len(rf.logs) - 1; n > rf.commitIndex; n-- {
					if rf.logs[n].Term != rf.currentTerm {
						continue
					}
					count := 0
					for _, mi := range rf.matchIndex {
						if mi >= n {
							count++
						}
					}
					if count >= len(rf.peers)/2+1 {
						rf.commitIndex = n
						break
					}
				}
			} else {
				// Log inconsistency: decrement nextIndex and retry (5.3)
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
			}
		}()
	}
}

func (rf *Raft) startHeartbeatLoop() {
	var ctx context.Context
	ticker := time.NewTicker(rf.heartbeatInterval)
	defer ticker.Stop()

	rf.mu.Lock()
	if rf.stopHeartbeat != nil {
		rf.stopHeartbeat()
	}
	ctx, rf.stopHeartbeat = context.WithCancel(context.Background())
	rf.mu.Unlock()

	go rf.sendHeartbeats()

	for {
		if rf.killed() {
			return
		}
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			rf.mu.Lock()

			if rf.role != LEADER {
				rf.stopHeartbeat()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go rf.sendHeartbeats()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return -1, -1, false
	}
	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = len(rf.logs) - 1
	rf.nextIndex[rf.me] = len(rf.logs)
	index := len(rf.logs) - 1
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) initElectionTimer() {
	originalTimeout := rf.electionTimeoutDuration

	jitteredDuration := originalTimeout + time.Duration(rf.rng.Int63n(int64(originalTimeout)))
	rf.electionTimeoutTicker = time.NewTicker(jitteredDuration)
	rf.Logger.Printf("Node initialized with (Election timeout: %v), (heartbeatInterval: %v)", jitteredDuration, rf.heartbeatInterval)

	go rf.handleElectionTimeout()
}

// resetElectionTimerLocked resets the election timer. Caller must hold rf.mu
func (rf *Raft) resetElectionTimerLocked() {
	originalTimeout := rf.electionTimeoutDuration
	jitteredDuration := originalTimeout + time.Duration(rf.rng.Int63n(int64(originalTimeout)))
	rf.Logger.Printf("Node Election timeout reset to: %v", jitteredDuration)
	rf.electionTimeoutTicker.Reset(jitteredDuration)
}

// election timeout logic
func (rf *Raft) handleElectionTimeout() {
	for {
		if rf.killed() {
			return
		}

		<-rf.electionTimeoutTicker.C

		rf.mu.Lock()

		if rf.role == LEADER {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		rf.Logger.Println("Node timeout, starting election")
		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.Logger.Print("Election Started")
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	ctx, cancelRequestVote := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancelRequestVote()

	if won := rf.requestVotes(ctx); won {
		rf.Logger.Print("Election Won")
		rf.mu.Lock()

		rf.role = LEADER
		rf.votedFor = -1
		lastLogIndex := len(rf.logs) - 1
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = lastLogIndex
		rf.mu.Unlock()
		go rf.startHeartbeatLoop()
	} else {
		rf.Logger.Print("Election Lost")
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
	}
}

// applier runs in the background and applies committed entries to the state machine (5.3).
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied || rf.lastApplied+1 >= len(rf.logs) {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
		}
		index := rf.lastApplied + 1
		cmd := rf.logs[index].Command
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: index}

		rf.mu.Lock()
		rf.lastApplied = index
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.rng = rand.New(rand.NewSource(time.Now().UnixNano()))

	// timing durations
	rf.electionTimeoutDuration = time.Millisecond * 200
	rf.heartbeatInterval = rf.electionTimeoutDuration / 2

	rf.Logger = log.New(io.Discard, fmt.Sprintf("[Node:%v]: ", rf.me), log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)

	// Log index 0 is dummy (paper: first index is 1)
	rf.logs = []Log{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.initElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()

	return rf
}
