package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Receiver implementation rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// Rule for all servers
	if args.Term > rf.currentTerm {
		rf.newTermL(args.Term)
	}
	// 收到心跳时更新选举时间
	rf.setElectionTime()
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.log.lastindex() || rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		reply.Success = false
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		DPrintf("%v: merge before, log = %v", rf.me, rf.log)
		rf.mergeLogL(args.PrevLogIndex+1, args.Entries)
		DPrintf("%v: merge done, log = %v", rf.me, rf.log)
	}
	if args.LeaderCommit > rf.commitIndex {
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		if args.LeaderCommit >= rf.log.lastindex() {
			rf.commitIndex = rf.log.lastindex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) mergeLogL(startIndex int, entries []Entry) {
	i, j := startIndex, 0
	for ; j < len(entries); i, j = i+1, j+1 {
		if i <= rf.log.lastindex() {
			if rf.log.entry(i).Term == entries[j].Term {
				continue
			}
			rf.log.cutend(i)
			rf.log.append(entries[j])
		} else {
			rf.log.append(entries[j])
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// Rule for leaders
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		if rf.log.lastindex() >= rf.nextIndex[i] || heartbeat {
			rf.sendAppendL(i, heartbeat)
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	next := rf.nextIndex[peer]
	if next <= rf.log.start() { // 跳过位置0的entry
		next = rf.log.start() + 1
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: next - 1,
		PrevLogTerm:  rf.log.entry(next - 1).Term,
		Entries:      make([]Entry, rf.log.lastindex()-next+1),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.log.slice(next))
	if !heartbeat {
		DPrintf("%v: send entry to %v, nextIndex[%v] = %v, args = %v, log = %v",
			rf.me, peer, peer, rf.nextIndex[peer], args, rf.log)
	}
	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.processAppendReplyL(peer, &args, &reply)
		}
	}()
}

func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		// Rule for all servers
		rf.newTermL(reply.Term)
	} else if rf.currentTerm == args.Term {
		rf.processAppendReplyTermL(peer, args, reply)
	}
}

func (rf *Raft) processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		// If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		newnext := args.PrevLogIndex + len(args.Entries) + 1
		newmatch := args.PrevLogIndex + len(args.Entries)
		if newnext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newnext
		}
		if newmatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newmatch
		}
		rf.advanceCommitL()
	} else {
		rf.nextIndex[peer]--
	}
}

func (rf *Raft) advanceCommitL() {
	start := rf.commitIndex + 1
	for index := start; index <= rf.log.lastindex(); index++ {
		if rf.log.entry(index).Term != rf.currentTerm {
			// 5.4.2
			continue
		}
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		n := 1 // leader always matches
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				n++
			}
		}
		if n > len(rf.peers)/2 {
			rf.commitIndex = index
		}
	}
	rf.signalApplierL()
}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Signal()
}
