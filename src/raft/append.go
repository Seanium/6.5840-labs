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
	Term       int
	Success    bool
	FirstIndex int // quick catch up for lab 2c
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

	// Reply false if Log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// quick catch up
	if args.PrevLogIndex > rf.log.lastindex() {
		reply.FirstIndex = -1
		reply.Success = false
		return
	}
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		conflictTerm := rf.log.entry(args.PrevLogIndex).Term
		var i int
		for i = args.PrevLogIndex; i > rf.log.start(); i-- {
			e := rf.log.entry(i)
			if e.Term != conflictTerm {
				break
			}
		}
		reply.FirstIndex = i + 1
		reply.Success = false
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		DPrintf("%v: merge before, Log = %v", rf.me, rf.log)
		rf.mergeLogL(args.PrevLogIndex+1, args.Entries)
		DPrintf("%v: merge done, Log = %v", rf.me, rf.log)
	}
	if args.LeaderCommit > rf.commitIndex {
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastindex())

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
	// Persist here
	rf.persist()
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
		// If last Log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with Log entries starting at nextIndex
		if rf.log.lastindex() >= rf.nextIndex[i] || heartbeat {
			rf.sendAppendL(i, heartbeat)
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	next := rf.nextIndex[peer]
	if next <= rf.log.start() { // 跳过位置0的entry
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		go func() {
			reply := InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(peer, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.newTermL(reply.Term)
					return
				}
				if rf.currentTerm != args.Term {
					return
				}
				if args.LastIncludedIndex+1 > rf.nextIndex[peer] {
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
				}
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			}
		}()
		return
	}
	var args AppendEntriesArgs
	if rf.log.lastindex() >= rf.nextIndex[peer] {
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.log.entry(next - 1).Term,
			Entries:      make([]Entry, rf.log.lastindex()-next+1),
			LeaderCommit: rf.commitIndex,
		}
		copy(args.Entries, rf.log.slice(next))
	} else {
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.log.lastindex(),
			PrevLogTerm:  rf.log.entry(rf.log.lastindex()).Term,
			LeaderCommit: rf.commitIndex,
		}
	}
	if !heartbeat {
		DPrintf("%v: send entry to %v, nextIndex[%v] = %v, args = %v, Log = %v",
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
		// slow catch up
		// rf.nextIndex[peer]--

		// quick catch up
		if reply.FirstIndex == -1 {
			var i int
			for i = args.PrevLogIndex; i > rf.log.start(); i-- {
				e := rf.log.entry(i)
				if e.Term != args.PrevLogTerm {
					break
				}
			}
			rf.nextIndex[peer] = min(i+1, rf.nextIndex[peer])
		} else {
			rf.nextIndex[peer] = min(reply.FirstIndex, rf.nextIndex[peer])
		}
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
		// of matchIndex[i] ≥ N, and Log[N].term == currentTerm:
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

func min(a int, b int) int {
	if a >= b {
		return b
	}
	return a
}
