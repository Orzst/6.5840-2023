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
	//	"bytes"

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A需要的
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	// lastApplied int
	nextIndex []int
	// matchIndex []int
	role                raftRole
	lastElectionTimeout time.Duration
	lastHeartbeatOrVote time.Time // 收到leader的heartbeat，或者投出了一票
}

// 表示raft peer身份的类型
type raftRole int

const (
	Follower raftRole = iota
	Candidate
	Leader
)

// 表示log的entry
type Entry struct {
	Term    int
	Index   int
	Command interface{} // 表示与具体service相关的操作
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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

	// 2A的部分
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.role = Follower
		}
		if rf.votedFor == -1 &&
			(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
				args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeartbeatOrVote = time.Now()
		} else {
			reply.VoteGranted = false
		}
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// server参数是rf.peers[]里目标服务器的索引
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
// 能正确收到响应的，就返回true。否则为false
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// 除了远程调用的函数没有返回的情况，Call()都能保证有返回值
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

// AppendEntries RPC的实现
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

	reply.Success = false
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.role = Follower
		}
		// 这里分两种，heartbeat和添加entry
		// heartbeat
		if len(args.Entries) == 0 {
			rf.lastHeartbeatOrVote = time.Now()
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		if (rf.role == Follower || rf.role == Candidate) &&
			time.Since(rf.lastHeartbeatOrVote) >= rf.lastElectionTimeout {
			go rf.startElection()
		}
		ms := 450 + (rand.Int63() % 150)
		d := time.Duration(ms) * time.Millisecond
		rf.lastElectionTimeout = d
		rf.mu.Unlock()

		time.Sleep(d)

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 发起election
func (rf *Raft) startElection() {
	// 过程中涉及rf的状态改变，那是不是都需要锁？先锁着
	// 不能偷懒锁整个过程，因为rpc调用里有一种情况是不会超时而一直等的，
	// 就是rpc函数运行被阻塞，一种可能的死锁就是A发起投票，要B选票
	// B选票过程要锁，但B可能也发起了投票，就死锁了
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	lastLogIndex := rf.log[len(rf.log)-1].Index
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.role = Candidate
	rf.votedFor = rf.me
	numVote := 1 // 自己一票
	rf.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					if reply.Term <= rf.currentTerm && reply.VoteGranted {
						numVote++
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}
				wg.Done()
			}(i)
		}
	}
	wg.Wait()

	rf.mu.Lock()
	if rf.role == Candidate && numVote > len(rf.peers)/2 {
		fmt.Printf("获得选票过半：%d\n", numVote)
		rf.role = Leader
		// rf.needElection = false
		rf.nextIndex = make([]int, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
		}
		// rf.matchIndex =
		go rf.heartbeatTicker()
	}
	rf.mu.Unlock()
	// 还有东西要写吗？好像暂时没了
}

// heartbeat间隔计时
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			break
		} else {
			go rf.heartbeat()
			rf.mu.Unlock()
		}

		ms := 20 // 题目限制一秒最多几十次的数量级
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 发出heartbeat
func (rf *Raft) heartbeat() {
	// 先加锁，因为后面可能修改rf状态
	// 之后看看别人怎么实现
	var wg sync.WaitGroup
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			wg.Add(1)
			go func(i int) {

				rf.mu.Lock()
				currentTerm := rf.currentTerm
				commitIndex := rf.commitIndex
				prevLogIndex := rf.nextIndex[i] - 1
				var prevLogTerm int
				for j := len(rf.log) - 1; j >= 0; j-- {
					if rf.log[j].Index == prevLogIndex {
						prevLogTerm = rf.log[j].Term
						break
					}
				}
				rf.mu.Unlock()

				args := AppendEntriesArgs{
					Term:         currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []Entry{},
					LeaderCommit: commitIndex,
				}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
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

	// Your initialization code here (2A, 2B, 2C).

	// 2A需要的
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Entry{{Index: 0, Term: -1}}
	rf.commitIndex = 0
	rf.role = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
