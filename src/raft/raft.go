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

	//"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Leader = iota
	Candidate
	Follower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	log         []LogEntry
	currentTerm int
	voteFor     int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state int

	timeOutDur int

	appendReqCh  chan int
	reqVoteCh    chan int
	changeRoleCh chan int

	electionTimer *time.Timer

	lastTime time.Time 
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = rf.state == Leader
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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
type LogEntry struct {
	Term    int
	Command string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntryHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("server %d append request to server %d on term %d", args.LeaderID, rf.me, args.Term)
	reply.Success = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		reply.Success = true
		rf.state = Follower
		rf.voteFor = -1
	}
	//rf.resetElectionTimer()
	rf.lastTime = time.Now()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntryHandler", args, reply)
	return ok

}



//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voteFor = -1
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateID {
		rf.voteFor = args.CandidateID
		reply.VoteGranted = true
		// rf.resetElectionTimer()
		rf.lastTime = time.Now()
	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := rf.lastApplied
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	//log.Printf("%d", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.timeOutDur = rand.Intn(150) + 350
	rf.appendReqCh = make(chan int)
	rf.reqVoteCh = make(chan int)
	rf.changeRoleCh = make(chan int)
	rf.electionTimer = time.NewTimer(rf.getTimerDuration())
	rf.lastTime = time.Now()
	//log.Printf("server %d has timeout %d", rf.me, rf.timeOutDur)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.serverRules()

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

func (rf *Raft) getTimerDuration() time.Duration {

	return time.Duration(rf.timeOutDur) * time.Millisecond

}

func (rf *Raft) resetElectionTimer(){

	rf.electionTimer.Reset(time.Duration(rf.timeOutDur) * time.Millisecond)

}

func (rf *Raft) serverRules() {

	for !rf.killed() {

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {

		case Leader:
			//rf.electionTimer.Stop()
			rf.startLeaderHeartBeats()
			time.Sleep(time.Millisecond * 100)
			break
		case Candidate:
			rf.startCandidateElection()
			break
		case Follower:
			rf.startFollower()
			break

		}

	}

}

func (rf *Raft) startLeaderHeartBeats() {

	for i := range rf.peers {
		rf.mu.Lock()
		if rf.state != Leader{
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}

}

func (rf *Raft) sendHeartBeat(server int) bool {

	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}

	rf.mu.Lock()
	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	//log.Printf("LEADER %d is sending heart beat to %d for term %d", rf.me, server, rf.currentTerm)
	rf.mu.Unlock()

	
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	//log.Printf("LEADER %d receives %v heart beat response from %d for term %d", rf.me, reply.Success, server, rf.currentTerm)
	rf.mu.Unlock()
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
		}
		return reply.Success
	}

	return false
}

func (rf *Raft) startCandidateElection() {

	rf.mu.Lock()
	rf.currentTerm++
	//log.Printf("server %d becomes candidate for term %d", rf.me, rf.currentTerm)
	totalVotes := 1
	processedVotes := 1
	rf.voteFor = rf.me
	majority := len(rf.peers)/2 + 1
	electionStartTime := time.Now()
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				getVote := rf.setUpSendRequestVote(server)
				rf.mu.Lock()
				processedVotes++
				if getVote {
					totalVotes++
				}
				cond.Broadcast()
				rf.mu.Unlock()
			}(i)
		}
	}

rf.mu.Lock()
for processedVotes!=len(rf.peers) && totalVotes < majority && rf.state == Candidate && time.Now().Sub(electionStartTime)<rf.getTimerDuration(){

	cond.Wait()
}

if rf.state == Candidate && totalVotes >= majority{
		rf.state = Leader
}
rf.mu.Unlock()


}

func (rf *Raft) startFollower() {

	
		for {
			rf.mu.Lock()
			timePassed := time.Now().Sub(rf.lastTime)
			rf.mu.Unlock()
			
			if timePassed>=rf.getTimerDuration(){
				rf.mu.Lock()
				rf.state = Candidate
				rf.mu.Unlock()
				return
			}

			time.Sleep(50 * time.Millisecond)

		}

		

}

func (rf *Raft) setUpSendRequestVote(server int) bool {

	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	rf.mu.Lock()
	args.CandidateID = rf.me
	args.LastLogIndex = rf.lastApplied
	args.LastLogTerm = rf.currentTerm
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
		}
		return reply.VoteGranted
	}
	return false

}
