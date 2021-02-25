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

	applyCh 	chan ApplyMsg

	log         []LogEntry        //log entries
	currentTerm int
	voteFor     int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state int      

	timeOutDur int
	
	applyMsgCond *sync.Cond

	lastTimestamp time.Time        
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
	Command interface{}
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
	ConflictTerm int
	ConflictIndex int
}

//
// RPC handler for append entry RPC
//
func (rf *Raft) AppendEntryHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower 
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.convert2Follower()
	}

	rf.lastTimestamp = time.Now()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
    // whose term matches prevLogTerm
	if args.PrevLogIndex>=len(rf.log) || (args.PrevLogIndex>=0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm){
	
		if args.PrevLogIndex>=len(rf.log){

			reply.ConflictIndex = len(rf.log)

		}else{
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for i, val := range rf.log{

				if val.Term == reply.ConflictTerm{
					reply.ConflictIndex = i 
					break
				}

			}
		}
		reply.Success = false
		return
	}
	
	// 3. If an existing entry conflicts with a new one (same index
	// 	but different terms), delete the existing entry and all that
	// 	follow it 

	for i := range args.Entries{

		if args.PrevLogIndex+1+i >= len(rf.log) || args.Entries[i].Term != rf.log[args.PrevLogIndex+1+i].Term{
			rf.log = append(rf.log[:args.PrevLogIndex+1+i], args.Entries[i:]...)
			break
		}
	}


	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex{

		lastEntryIndex := len(rf.log)-1

		if args.LeaderCommit > lastEntryIndex{
			rf.commitIndex = lastEntryIndex
		}else{
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyMsgCond.Broadcast()

	}

	//rf.printAllStats("append entry success")
	reply.Term = rf.currentTerm
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

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.convert2Follower()
	}

	higherTermCheck := true
	higherIndexCheck := true

	if len(rf.log)>0{
		lastLogEntry := rf.log[len(rf.log) - 1]
		higherTermCheck = args.LastLogTerm > lastLogEntry.Term
		higherIndexCheck = (args.LastLogTerm == lastLogEntry.Term) && (args.LastLogIndex >= len(rf.log)-1)
	}

    // 2. If votedFor is null or candidateId, and candidate’s log is at
    // least as up-to-date as receiver’s log, grant vote
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateID) && (higherTermCheck || higherIndexCheck){
		rf.voteFor = args.CandidateID
		reply.VoteGranted = true
		rf.lastTimestamp = time.Now()
	}

	reply.Term = rf.currentTerm

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	// Your code here (2B).
	if isLeader{
		newEntry := LogEntry{
			Term: rf.currentTerm,
			Command: command,
		}
		index = len(rf.log)
		// If command received from client: append entry to local log
		rf.log = append(rf.log, newEntry)
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}

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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteFor = -1
	rf.state = Follower
	rf.log = make([] LogEntry, 1)
	rf.applyCh = applyCh
	
	rf.nextIndex = make([] int, len(peers))
	rf.matchIndex = make([] int, len(peers))
	rf.timeOutDur = rand.Intn(250) + 400
	rf.lastTimestamp = time.Now()

	rf.applyMsgCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startMainRoutine()
	go rf.applyCommittedEntries()

	return rf
}

func (rf *Raft) updateCommitIndex(){

	for i:=len(rf.log)-1 ; i>rf.commitIndex ; i-- {	
			count := 0
			for _, val := range rf.matchIndex{
				if val >= i{
					count++
					if (count > len(rf.matchIndex)/2) && (rf.log[i].Term == rf.currentTerm){
							rf.commitIndex = i
							rf.applyMsgCond.Broadcast()
							return
					}
				}
			}
		
	}

}


func (rf *Raft) applyCommittedEntries(){


	for !rf.killed(){

		rf.mu.Lock()
		rf.applyMsgCond.Wait()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		if lastApplied < commitIndex{
			log := make([] LogEntry, commitIndex-lastApplied)
			copy(log, rf.log[lastApplied+1:commitIndex+1])

			for i:=0;i<len(log);i++ {
	
				newApplyMsg := ApplyMsg{
	
					CommandValid: true,
					Command: log[i].Command,
					CommandIndex: lastApplied + i + 1,

				}
				rf.mu.Lock()
				rf.lastApplied = i
				rf.mu.Unlock()
				rf.applyCh <- newApplyMsg

			}
		
		}

	}



}

// get the election time duration 
func (rf *Raft) getTimerDuration() time.Duration {

	return time.Duration(rf.timeOutDur) * time.Millisecond

}

// randomize the election time duration 
func (rf *Raft) randomizeTimerDuration(){

	rf.timeOutDur = rand.Intn(250) + 400

}

// start the main raft server task 
func (rf *Raft) startMainRoutine() {

	for !rf.killed() {

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {

		case Leader:
			rf.startLeaderHeartBeats()
			time.Sleep(time.Millisecond * 100)
			break
		case Candidate:
			rf.startCandidateElection()
			break
		case Follower:
			rf.startFollower()
			break
		default:
			panic("No such state !")

		}

	}

}

// leader sends heart beats to all peers periodically 
func (rf *Raft) startLeaderHeartBeats() {

	for i := range rf.peers {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed(){
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if i != rf.me {
			go rf.sendHeartBeat(i)
		}
	}

}

// leader's helper function to send heart beats to peers
func (rf *Raft) sendHeartBeat(server int) bool {

	args := AppendEntriesArgs{}
	reply := AppendEntriesReply{}	
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LeaderCommit = rf.commitIndex   
	args.PrevLogIndex = rf.nextIndex[server]-1

	if args.PrevLogIndex>=0{
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	// If last log index ≥ nextIndex for a follower
	if len(rf.log) > rf.nextIndex[server]{
		newEntries := make([] LogEntry, len(rf.log)-rf.nextIndex[server])
		copy(newEntries, rf.log[rf.nextIndex[server]:])
		args.Entries = newEntries
	}
	rf.mu.Unlock()
	//rf.printAllStats("before send heart beat")
	ok := rf.sendAppendEntries(server, &args, &reply)

	if ok{

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.convert2Follower()
			return false
		}
		if args.Term != reply.Term{
			return false
		}

		if reply.Success{
			//rf.printAllStats("send heart beat receive success")
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.updateCommitIndex()
			return true

		}

		if reply.ConflictTerm != -1{

			found := false

			for i := args.PrevLogIndex ; i>0 ; i--{

				if rf.log[i].Term == reply.ConflictTerm{

					rf.nextIndex[server] = i+1
					found = true
					break
				}

			}

			if !found{
				rf.nextIndex[server] = reply.ConflictIndex;
			}

		}else{

			rf.nextIndex[server] = reply.ConflictIndex;

		}	

		

	}

return false 
}

// candidate will start an election 
func (rf *Raft) startCandidateElection() {

	rf.mu.Lock()
	rf.randomizeTimerDuration()
	rf.currentTerm++
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
				if getVote{
					totalVotes++
				}
				cond.Broadcast()
				rf.mu.Unlock()
			}(i)
		}
	}

rf.mu.Lock()
defer rf.mu.Unlock()

for processedVotes!=len(rf.peers) && totalVotes < majority && rf.state == Candidate && time.Now().Sub(electionStartTime)<rf.getTimerDuration(){

	cond.Wait()
}

// if candidate gets majority of votes, then beomces a leader 
if rf.state == Candidate && totalVotes >= majority{
		rf.convert2Leader()
}


}

// request vote set up helper function for candidate election 
func (rf *Raft) setUpSendRequestVote(server int) bool {

	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	rf.mu.Lock()
	args.CandidateID = rf.me
	args.LastLogIndex = len(rf.log)-1
	if len(rf.log)>0{
		args.LastLogTerm = rf.log[args.LastLogIndex].Term
	}
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	ok := rf.sendRequestVote(server, &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.convert2Follower()
			return false
		}
		return reply.VoteGranted
		
	}
	return false

}

// follower main task 
func (rf *Raft) startFollower() {

	
		for !rf.killed(){

			rf.mu.Lock()
			timePassed := time.Now().Sub(rf.lastTimestamp)
			
			// if election timeout, then follower becomes a candidate
			if timePassed>=rf.getTimerDuration(){
				rf.state = Candidate
				rf.mu.Unlock()
				return
			}

			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)

		}


}

// helper function to convert to a follower 
func (rf *Raft) convert2Follower(){
	//log.Printf("SERVER %v becomes FOLLOWER\n", rf.me)
	rf.state = Follower
	rf.voteFor = -1
	rf.randomizeTimerDuration()

}

func (rf *Raft) convert2Leader(){

	//log.Printf("SERVER %v becomes LEADER\n", rf.me)
	rf.state = Leader 
	rf.leaderStateInit()

}

func (rf *Raft) leaderStateInit(){
	//rf.printAllStats("leader init ")
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log)-1

	for i := range rf.peers{

		if i!=rf.me{
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}

	}



}


func (rf *Raft) printAllStats(header string){

	//log.Printf("%v : Server %v with state %v: currentTerm: %v, commitIndex: %v, lastApplied: %v, nextIndex %v, matchIndex %v \n", header, rf.me, rf.state, rf.currentTerm, rf. commitIndex, rf.lastApplied,rf.nextIndex, rf.matchIndex);
}
