package raft

type AppendEntriesArgs struct {
	Term int
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
	reply.Success = true
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
		if heartbeat {
			rf.sendAppendL(i, heartbeat)
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	args := AppendEntriesArgs{Term: rf.currentTerm}
	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Rule for all servers
			if reply.Term > rf.currentTerm {
				rf.newTermL(reply.Term)
			}
		}
	}()
}
