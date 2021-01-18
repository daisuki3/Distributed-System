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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"
// import "bytes"
// import "../labgob"


const NotVoted int = -100
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term int
}
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

	ch chan ApplyMsg
	//persistent
	currentTerm int
	votedFor int
	log []LogEntry
	
	//volatile on each server
	commitIndex int
	lastApplied int

	//volative on leader to track
	nextIndex []int
	mathchIndex []int

	//for live check
	leaderIndex int
	heartbeatTime time.Time

	//for random election timeout 
	electionNum int
	
	startmx sync.Mutex
	beatLock sync.Mutex
	mx sync.Mutex
	applyMx sync.Mutex
	cond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mx.Lock()
	term = rf.currentTerm
	isleader = rf.me == rf.leaderIndex
	rf.mx.Unlock()
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	ElectionNum int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
		Term int
		VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mx.Lock()
	defer rf.mx.Unlock()
	//stale leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//leader heartbeat
	rf.leaderIndex = args.LeaderId
	rf.votedFor = NotVoted
	
	reply.Term = rf.currentTerm
	reply.Success = true
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	//update follower commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if len(rf.log) - 1 >= rf.commitIndex + 1 {
			rf.cond.Broadcast()
		}
	}
	//only heartbeat
	if len(args.Entries) == 0 {
		rf.beatLock.Lock()
		rf.heartbeatTime = time.Now()
		rf.beatLock.Unlock()

		rf.electionNum = 1
		//DPrintf("heart beat from %v to %v", args.LeaderId, rf.me)
		return
	}

	//leader append entry	
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return 
	}

	if args.Term < rf.currentTerm || (args.PrevLogIndex != 0 && args.PrevLogIndex < len(rf.log) &&
	 rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = false
		return  
	}

	
	
	//DPrintf("leader %v send log %v to server %v which now is %v", args.LeaderId, args.Entries, rf.me, rf.log)
	if args.PrevLogIndex + len(args.Entries) <= len(rf.log) - 1 {
		reply.Success = false
		for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex + len(args.Entries); i++ {
			if args.Entries[i - args.PrevLogIndex - 1] != rf.log[i] {
				reply.Success = true			
			}
		}
	} 
	
	if reply.Success == true {
		rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
		DPrintf("server %v now is %v", rf.me, rf.log)
	}


}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("%v requst vote from %v, arg term %v me term %v arg %v %v  me %v %v,", args.CandidateId, 
	rf.me, args.Term, rf.currentTerm, args.LastLogTerm, args.LastLogIndex, rf.log[len(rf.log) - 1].Term, len(rf.log) - 1)

	rf.mx.Lock()
	defer rf.mx.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	loglen := len(rf.log)


	if rf.votedFor == NotVoted || rf.votedFor == args.CandidateId {
		if args.Term < rf.currentTerm {
			reply.VoteGranted = false
		} else if loglen == 1 || args.LastLogTerm > rf.log[loglen - 1].Term ||
			(args.LastLogTerm == rf.log[loglen - 1].Term && 
			args.LastLogIndex >= len(rf.log) - 1) {
			
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

	} else if args.ElectionNum > rf.electionNum {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.startmx.Lock()
	defer rf.startmx.Unlock()
	rf.mx.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader :=  rf.leaderIndex == rf.me
	rf.mx.Unlock()

	// Your code here (2B).
	if isLeader == true {

		if command != rf.log[len(rf.log) - 1].Command {
			rf.mx.Lock()
			rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
			rf.mx.Unlock()
			DPrintf("leader %v now is %v", rf.me, rf.log)
		} else {
			index = len(rf.log) - 1
		}
		
		
		for i := 0 ; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(target int) {
				//appendentry for one server until success
				for {
					if rf.mathchIndex[target] >= index {
						break
					}
					rf.mx.Lock()
					args := AppendEntriesArgs{}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[target] - 1		
					args.PrevLogTerm = rf.log[rf.nextIndex[target] - 1].Term
					args.Entries = rf.log[args.PrevLogIndex + 1:]
					//args.Entries = []LogEntry {{rf.log[rf.nextIndex[target]].Command, rf.log[rf.nextIndex[target]].Term}}
					args.LeaderCommit = rf.commitIndex
					rf.mx.Unlock()

					reply := AppendEntriesReply{}
				
					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						rf.sendAppendEntries(target, &args, &reply)
					}()
					wg.Wait()
					
			
					if reply.Success == true {
						DPrintf("leader %v append to server %v with %v", rf.me, target, args.Entries)
						rf.mx.Lock()
						loglen := len(rf.log)
						rf.mathchIndex[target] = loglen - 1
						rf.nextIndex[target] = loglen
						newCommit := rf.commitIndex + 1
						rf.mx.Unlock()

						if newCommit >= loglen {
							break
						}

						majority := (len(rf.peers) - 1) / 2 + 1
						commitNum := 0
						
						for j := 0; j < len(rf.peers); j++ {
							if j == rf.me || rf.mathchIndex[j] >= newCommit {
								commitNum += 1
							} 
						}

						if commitNum >= majority && rf.log[newCommit].Term == rf.currentTerm {
							rf.cond.Broadcast()
						}

						break

					} else {
						if rf.nextIndex[target] != 1{
							rf.nextIndex[target] = rf.nextIndex[target] - 1 
						}
					}

					time.Sleep(time.Millisecond * 100)
				}

			}(i)
		}
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
	
	rf.currentTerm = 1
	rf.log = []LogEntry{LogEntry{0, 0}}
	rf.ch = applyCh
	rf.votedFor = NotVoted
	
	rf.heartbeatTime = time.Now()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.mathchIndex = make([]int, len(rf.peers))
	rf.cond = sync.NewCond(&rf.applyMx)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.mathchIndex[i] = 0
	}

	go func(){
			//heartbeat goroutine
			for{
				// 1s = 1000 milliseconds 1000ms
				//leader send heartbeats, at most 10 heartbeats/s
				rf.mx.Lock()
				isLeader := rf.me == rf.leaderIndex
				rf.mx.Unlock()
				if isLeader == true {
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
				
						go func(i int){ 
							args := AppendEntriesArgs{}
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.LeaderCommit = rf.commitIndex
							
							go func(){
								rf.sendAppendEntries(i, &args, &AppendEntriesReply{})
							}()
						}(i)
					}

					//DPrintf("%v heartbeat go", rf.me)
				}
						
				time.Sleep(100 * time.Millisecond)
				
			}
	}()
	

	go func(){
		//election goroutine 
		heartbeatTime := time.Now()
		for {

			time.Sleep(time.Second)
			rf.beatLock.Lock()
			heartbeatTime = rf.heartbeatTime
			rf.beatLock.Unlock()
			isTimeout := make(chan bool)
			if rf.me != rf.leaderIndex && time.Now().After(heartbeatTime.Add(time.Millisecond * 700)) {
				rf.mx.Lock()
				rf.electionNum += 1
				rf.votedFor = rf.me
				rf.mx.Unlock()
				DPrintf("%v start election bearttime %v now %v ,out of time ? %v ", rf.me, heartbeatTime, time.Now(), time.Now().After(heartbeatTime.Add(time.Millisecond * 700)))
				//timeout 250 ~ 400 Millisecond
				//random timeout 250 - 400 ms

				go func(isTimeout chan bool) {
					timeout := rand.Int() % 150 + 1 + 250 
					time.Sleep(time.Duration(timeout) * time.Millisecond)
					isTimeout <- true
				}(isTimeout)
			
				majority := (len(rf.peers) - 1) / 2 + 1
				voteGot := 1
				var mutex sync.Mutex 
			
				//start election poll
				for i := 0; i < len(rf.peers); i++ {			
					if i == rf.me {
						continue
					}

					args := RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateId = rf.me
					args.LastLogIndex = len(rf.log) - 1
					args.LastLogTerm = rf.log[len(rf.log) - 1].Term
					args.ElectionNum = rf.electionNum
					reply := RequestVoteReply{}
					reply.VoteGranted = false
					go func(i int){
						var wg sync.WaitGroup
						wg.Add(1)
						go func(){
							defer wg.Done()
							rf.sendRequestVote(i, &args, &reply)
						}()
						
						//for synchronization
						wg.Wait()
						if reply.VoteGranted == true {		
							mutex.Lock()
							voteGot = voteGot + 1
							if voteGot >= majority && rf.me != rf.leaderIndex {

								rf.mx.Lock()
								rf.leaderIndex = rf.me
								rf.currentTerm = rf.currentTerm + 1 
								
								//track follower's log state
								rf.nextIndex = make([]int, len(rf.peers))
								rf.mathchIndex = make([]int, len(rf.peers))
								for i := 0; i < len(rf.peers); i++ {
									rf.nextIndex[i] = len(rf.log)
									rf.mathchIndex[i] = 0
								}
								rf.mx.Unlock()

								DPrintf("term %v got leader %v", rf.currentTerm, rf.me)
							}
							mutex.Unlock()
						}
					}(i)
				}
			}
			<- isTimeout
		}		
	}()
	
	go func(){
		//commit routine

		for {
			rf.cond.L.Lock()
			
			for rf.commitIndex + 1 > len(rf.log) - 1 {
				rf.cond.Wait()
			}

			rf.commitIndex += 1
			msg := ApplyMsg{}
			msg.Command = rf.log[rf.commitIndex].Command
			msg.CommandIndex = rf.commitIndex
			msg.CommandValid = true
			DPrintf("server %v commit %v", rf.me, rf.commitIndex)
			rf.ch <- msg
			rf.cond.L.Unlock()
		}
	}()
	return rf
}
