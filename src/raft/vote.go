package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rule for all servers
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}

	// For rule 2, compute leader restriction
	myIndex := rf.log.lastindex()
	myTerm := rf.log.entry(myIndex).Term
	// If the logs have last entries with different terms, then
	// the Log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever Log is longer is
	// more up-to-date.
	uptodate := (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex) || args.LastLogTerm > myTerm

	if args.Term < rf.currentTerm {
		// Rule 1. Reply false if term < currentTerm
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		// Rule 2. If votedFor is null or candidateId, and candidate’s Log is at
		// least as up-to-date as receiver’s Log, grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// Persist here
		rf.persist()
		// Rule for followers
		rf.setElectionTime()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) becomeLeaderL() {
	DPrintf("%v: term %v become leader\n", rf.me, rf.currentTerm)
	rf.state = LEADER
	for i := range rf.matchIndex {
		// for each server, index of the next Log entry
		// to send to that server (initialized to leader
		// last Log index + 1)
		rf.nextIndex[i] = rf.log.lastindex() + 1
	}
	DPrintf("%v: initialize nextIndex = %v", rf.me, rf.nextIndex)
}

func (rf *Raft) sendRequestVoteAndCountVotes(peer int, args *RequestVoteArgs, votes *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// Rule for all servers
		if reply.Term > rf.currentTerm {
			rf.newTermL(reply.Term)
		}
		if reply.VoteGranted {
			*votes++
			if *votes > len(rf.peers)/2 {
				// 保证与发起选举时处于同样的term
				if rf.currentTerm == args.Term {
					// Once a candidate wins an election, it
					// becomes leader. It then sends heartbeat messages to all of
					// the other servers to establish its authority and prevent new
					// elections.
					rf.becomeLeaderL()
					rf.sendAppendsL(true)
				}
			}
		}
	}
}

// 设置下次选举时间为当前时间的1000~1300ms后随机值
func (rf *Raft) setElectionTime() {
	t := time.Now()
	ms := 1000 + (rand.Int63() % 300)
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
	// DPrintf("%v: setElectionTime: %v", rf.me, rf.electionTime)
}

func (rf *Raft) newTermL(term int) {
	DPrintf("%v: term %v become follower\n", rf.me, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = FOLLOWER
	// Persist here
	rf.persist()
}
