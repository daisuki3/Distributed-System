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
import "bytes"
import "../labgob"


const NotVoted int = -100
const NoLeader int = -101
const NoTerm int = -102
const Leader int = -11
const Follower int = 1
const Candidate int = 2
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
	votedForElection int
	log []LogEntry
	
	//volatile on each server
	committing bool
	commitIndex int
	lastApplied int

	//volative on leader to track
	nextIndex []int
	mathchIndex []int

	//for live check
	heartbeatTime time.Time
	heartbeatRecord []bool
	state int

	//for random election timeout 
	electionNum int

	startmx sync.Mutex
	beatLock sync.Mutex
	mx sync.Mutex
	applyMx sync.Mutex
	leaseMx sync.Mutex
	electionLock sync.Mutex
	cond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mx.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.votedForElection)
	e.Encode(rf.log)
	e.Encode(rf.electionNum)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var votedForElection int
	var log []LogEntry
	var electionNum int 
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&votedForElection) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&electionNum) != nil {
			DPrintf("persist read wrong")
			return 
		} else {
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.votedForElection = votedForElection
			rf.log = log
			rf.electionNum = electionNum
		}
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
		Length int
		LastIndexOfTerm int
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
	DoesNeedAdjustment bool
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mx.Lock()
	defer rf.mx.Unlock()
	//stale leader
	if args.Term < rf.currentTerm {
		DPrintf("stale leader %v term %v", args.LeaderId, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//leader heartbeat
	rf.votedFor = NotVoted
	rf.votedForElection = NotVoted
	rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = true
	
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}


 	if rf.state == Candidate {
		//DPrintf("server %v stop candidate", rf.me)
	} else if rf.state == Leader {
		//DPrintf("leader %v become follower", rf.me)
	}

	rf.beatLock.Lock()
	rf.heartbeatTime = time.Now()
	rf.beatLock.Unlock()
	rf.state = Follower
	rf.electionNum = 1

	//DPrintf("heartbeat arive ! server %v , loglen %v prevlogindex %v commit %v leadercommit %v --- ", rf.me,
//		len(rf.log), args.PrevLogIndex ,rf.commitIndex, args.LeaderCommit)

	//2 : 5
	//3: 5
	//4: 5
	//0: 6
	for args.LeaderCommit > rf.commitIndex &&
		len(rf.log) - 1 >= rf.commitIndex + 1 &&
		args.PrevLogIndex > rf.commitIndex {
		rf.committing = true
		rf.cond.Broadcast()
	}

	//leader append entry	
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.DoesNeedAdjustment = true
		rf.persist()
		return 
	}

	if args.PrevLogIndex > 0 && args.PrevLogIndex < len(rf.log) &&
	 rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[: args.PrevLogIndex]
		reply.Success = false
		reply.DoesNeedAdjustment = true
		rf.persist()
		return  
	}
	
	if args.PrevLogIndex + len(args.Entries) <= len(rf.log) - 1 {
		reply.Success = false
		for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex + len(args.Entries); i++ {
			if args.Entries[i - args.PrevLogIndex - 1] != rf.log[i] {
				reply.Success = true			
			}
		}
	} 

	if reply.Success == true {
		rf.log = append(rf.log[: args.PrevLogIndex + 1], args.Entries...)
		rf.persist()
	}
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mx.Lock()
	defer rf.mx.Unlock()
	//DPrintf("vote request : server %v arrive in server %v ", args.CandidateId, rf.me)
	reply.Term = rf.currentTerm
	loglen := len(rf.log)
	reply.Length = loglen

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if loglen == 1 || 
	args.LastLogTerm > rf.log[loglen - 1].Term ||
		(args.LastLogTerm == rf.log[loglen - 1].Term && args.LastLogIndex >= loglen - 1) {

			if args.ElectionNum > rf.electionNum && args.ElectionNum > rf.votedForElection {
			//	DPrintf("server %v term %v got vote from %v term %v, timeout and wakeup, electionNum %v me %v", args.CandidateId, args.Term,
			//		rf.me, rf.currentTerm, args.ElectionNum, rf.electionNum)
				rf.votedFor = args.CandidateId
				rf.votedForElection = args.ElectionNum
				reply.VoteGranted = true
				rf.persist()
			} else if args.ElectionNum == rf.electionNum &&
				(rf.votedFor == NotVoted || rf.votedFor == args.CandidateId) {
			//	DPrintf("server %v term %v got vote from %v  term %v, more up-to-date", args.CandidateId, args.Term,
			//		rf.me, rf.currentTerm)
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.persist()
			}

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
	isLeader := rf.state == Leader
	rf.mx.Unlock()

	// Your code here (2B).
	if isLeader == true {
		if command != rf.log[len(rf.log) - 1].Command {
			rf.mx.Lock()
			rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
			rf.persist()
			DPrintf("leader %v len %v term %v", rf.me, len(rf.log), rf.currentTerm)
			rf.mx.Unlock()
		} else {
			index = len(rf.log) - 1
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
func (rf *Raft) sendHeartBeat() {

	var appendWg sync.WaitGroup

	for i := 0 ; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		appendWg.Add(1)
		appendTimeout := make(chan bool)

		go func(timeout chan bool) {
			time.Sleep(time.Millisecond * 100)
			timeout <- true
		}(appendTimeout)

		go func(target int) {
			rf.mx.Lock()
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[target] - 1
			args.PrevLogTerm = rf.log[rf.nextIndex[target] - 1].Term

			args.Entries = rf.log[args.PrevLogIndex + 1:]
			args.LeaderCommit = rf.commitIndex
			loglen := len(rf.log)
			rf.mx.Unlock()

			reply := AppendEntriesReply{}
			reply.Term = NoTerm

			var wg sync.WaitGroup
			wg.Add(1)
			go func(){
				startTime := time.Now()
				for time.Now().After(startTime.Add(time.Millisecond * 25)) == false {
					rf.sendAppendEntries(i, &args, &reply)
					time.Sleep(time.Millisecond * 26)
				}
				wg.Done()
			}()
			wg.Wait()

			if reply.Success == true {

				rf.mx.Lock()
				rf.heartbeatRecord[target] = true
				if loglen > rf.nextIndex[target] {
					DPrintf("new next %v for server %v, old %v", loglen, target, rf.nextIndex[target])
					rf.mathchIndex[target] = loglen - 1
					rf.nextIndex[target] = loglen
				}
				rf.mx.Unlock()

				newCommit := rf.mathchIndex[target]

				if newCommit <= loglen - 1 {
					majority := (len(rf.peers) - 1) / 2 + 1
					commitNum := 0

					rf.mx.Lock()
					for j := 0; j < len(rf.peers); j++ {
						if j == rf.me || rf.mathchIndex[j] >= newCommit {
							commitNum += 1
						}
					}
					rf.mx.Unlock()

					if commitNum >= majority && rf.log[newCommit].Term == rf.currentTerm {
						for rf.commitIndex < newCommit {
							rf.committing = true
							rf.cond.Broadcast()
							time.Sleep(time.Millisecond * 2)
						}
					}
				}
			} else {
				rf.mx.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower
				} else if rf.nextIndex[target] != 1 && reply.DoesNeedAdjustment == true {
					rf.nextIndex[target] = rf.nextIndex[target] - 1
				}
				rf.mx.Unlock()
			}
		}(i)
		<-appendTimeout
		appendWg.Done()
	}
	appendWg.Wait()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mx.Lock()
	rf.electionLock.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.electionNum = 1
	rf.currentTerm = 0
	rf.log = []LogEntry{LogEntry{0, 0}}
	rf.ch = applyCh
	rf.votedFor = NotVoted
	rf.votedForElection = NotVoted

	rf.beatLock.Lock()
	rf.heartbeatTime = time.Now()
	rf.beatLock.Unlock()

	rf.committing = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.mathchIndex = make([]int, len(rf.peers))
	rf.heartbeatRecord = make([]bool, len(rf.peers))
	rf.cond = sync.NewCond(&rf.applyMx)
	rf.state = Follower
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.mathchIndex[i] = 0
		rf.heartbeatRecord[i] = false
	}
	rf.electionLock.Unlock()
	rf.mx.Unlock()

	//heartbeat
	go func (){
			//heartbeat goroutine
			state := Follower
			for{
				// 1s = 1000 milliseconds 1000ms
				//leader send heartbeats, at most 10 heartbeats/s
				time.Sleep(100 * time.Millisecond)
				rf.mx.Lock()
				state = rf.state
				rf.mx.Unlock()
				if state == Leader {
					rf.sendHeartBeat()
				}		

			}
	}()


	//Leader Lease
	go func(){
		state := 212
		for {              
			rf.leaseMx.Lock()   
			rf.mx.Lock()
			state = rf.state
			rf.mx.Unlock()
			if state == Leader {
				//DPrintf("enter expire me %v state %v", rf.me, rf.state)
				time.Sleep(time.Second * 4)
				rf.mx.Lock()
				majority := (len(rf.peers) - 1) / 2 + 1
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.heartbeatRecord[i] == true || rf.me == i {
						count += 1
					}
				}

				if count < majority && rf.state == Leader {
					rf.state = Follower
					//DPrintf("leader %v lease expires", rf.me)
				}

				for i := 0; i < len(rf.peers); i++ {
					rf.heartbeatRecord[i] = false
				}
				rf.mx.Unlock()
			} else {
				time.Sleep(time.Millisecond * 500)
			}
			rf.leaseMx.Unlock()
		}
	}()

	//election goroutine
	go func(){
		heartbeatTime := time.Now()
		state := rf.state
		for {
			time.Sleep(time.Millisecond * 300)
			rf.beatLock.Lock()
			heartbeatTime = rf.heartbeatTime
			rf.beatLock.Unlock()
			
			rf.mx.Lock()
			state = rf.state
			rf.mx.Unlock()
			if rf.electionNum > 0 &&
			state != Leader && time.Now().After(heartbeatTime.Add(time.Millisecond * time.Duration(500 + rand.Int() % 2))) {
				rf.electionLock.Lock()
				isTimeout := make(chan bool)
				rf.mx.Lock()
				rf.electionNum += 1
				if rf.electionNum > 5 {
					rf.electionNum = 1
				}
				rf.votedFor = rf.me
				rf.state = Candidate
				rf.persist()
				rf.mx.Unlock()
				//timeout 250 ~ 400 Millisecond
				//random timeout 250 - 500 ms
				go func(isTimeout chan bool) {
					timeout := rand.Int() % 150 + 1 + 200
					time.Sleep(time.Duration(timeout) * time.Millisecond)
					isTimeout <- true
				}(isTimeout)
			
				majority := (len(rf.peers) - 1) / 2 + 1
				voteGot := 1
				var mutex sync.Mutex 
			
				//start election poll

				for i := 0; i < len(rf.peers) && rf.electionNum > 0; i++ {
					time.Sleep(time.Millisecond)
					if i == rf.me {
						continue
					}
					rf.mx.Lock()
					state = rf.state
					rf.mx.Unlock()
					if state != Candidate {
						break
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
						var sendWg sync.WaitGroup
						sendWg.Add(1)
						go func(){
							startTime := time.Now()
							var once sync.Once
							for time.Now().After(startTime.Add(time.Millisecond * 90)) == false {
								once.Do(func (){
									rf.sendRequestVote(i, &args, &reply)
								})
								time.Sleep(time.Millisecond * 91)
							}
							sendWg.Done()
						}()
						sendWg.Wait()
						//DPrintf("%v got vote from %v ? %v", rf.me, i, reply.VoteGranted)
						if reply.VoteGranted == true {
							mutex.Lock()
							voteGot = voteGot + 1
							rf.mx.Lock()
							state = rf.state
							rf.mx.Unlock()
							if voteGot >= majority && state == Candidate {
								rf.mx.Lock()
								rf.state = Leader
								rf.currentTerm = rf.currentTerm + 1 
								rf.electionNum = 1
								rf.persist()
								//track follower's log state
								for i := 0; i < len(rf.peers); i++ {
									rf.nextIndex[i] = 1
									rf.mathchIndex[i] = 0
								}
								rf.mx.Unlock()
								DPrintf("------- term %v got leader %v --------- \n", rf.currentTerm, rf.me)
								var winWg sync.WaitGroup
								winWg.Add((1))
								go func(){
									startTime := time.Now()
									var once sync.Once
									for time.Now().After(startTime.Add(time.Millisecond * 50)) == false {
										once.Do(func (){
											rf.sendHeartBeat()
										})
										time.Sleep(time.Millisecond * 50)
									}
									winWg.Done()
								}()
								winWg.Wait()
							}
							mutex.Unlock()
						} else if reply.Term > rf.currentTerm || 
							(reply.Term == rf.currentTerm &&
								 reply.Length > len(rf.log)) {
							rf.mx.Lock()
							rf.electionNum = -100
							rf.votedForElection = -100
							rf.persist()
							rf.mx.Unlock()
						}
					}(i)
				}
				<- isTimeout
				rf.electionLock.Unlock()
			}
		}		
	}()

	//commit routine
	go func(){
		for {
			rf.cond.L.Lock()

			for rf.committing == false {
				rf.cond.Wait()
			}

			rf.commitIndex += 1
			DPrintf("server %v committing goroutine running ready commit %v ,loglen %v", rf.me, rf.commitIndex, len(rf.log))
			msg := ApplyMsg{}
			msg.Command = rf.log[rf.commitIndex].Command
			msg.CommandIndex = rf.commitIndex
			msg.CommandValid = true
			rf.ch <- msg
			rf.committing = false
			rf.cond.L.Unlock()
		}
	}()

	return rf
}
