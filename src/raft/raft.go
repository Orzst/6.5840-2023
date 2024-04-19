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
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	// lastApplied int
	nextIndex  []int
	matchIndex []int

	role                raftRole
	lastElectionTimeout time.Duration
	lastHeartbeatOrVote time.Time // 收到leader的heartbeat，或者投出了一票
	applyCh             chan ApplyMsg
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

	// 不只是不匹配是false，term过时也是false
	reply.Success = false
	if args.Term >= rf.currentTerm {
		// 每次通信总是要检查term
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.role = Follower
		}
		// 没有条目的heatbeat不该单独处理，有没有都一样处理就行
		// 按有条目的逻辑写完记得检查一下空条目的表现正不正常
		rf.lastHeartbeatOrVote = time.Now()
		var pos int
		for pos = len(rf.log) - 1; pos >= 0; pos-- {
			if rf.log[pos].Index == args.PrevLogIndex {
				if rf.log[pos].Term == args.PrevLogTerm {
					break

					// figure 2里说冲突的及其后要删掉
				} else {
					rf.log = rf.log[:pos]
				}
			}
		}
		if pos != -1 {
			reply.Success = true
			// guide提到，文章其实隐含的意思有不能直接截断到pos然后拼上args.Entries
			// 因为有可能同一个term中，先发出的rpc后到，此时有更新的entry的rpc可能已经完成了更新
			// 所以应该只添加没有的。
			// 我的理解是，raft的设计是AppendEntries只需要听来自Leader的请求，对应这里的
			// args.Term >= rf.currentTerm，它保证只处理来自可能的Leader的请求。假如每个Leader
			// 的请求被接收的顺序和发出顺序一致，截断到pos再拼上args.Entries是不会有问题的
			// （不同Leader的请求交错不要紧，因为旧Leader的请求出现在新Leader后由arg.Term >= rf.currentTerm保证被忽略）
			// 问题来自于并发以及网络的原因，同一Leader的请求收到的顺序未必与发出顺序一致。
			// 所以额外的处理也是针对这个问题。也就是说，要考虑的是如果同一个Leader后发的请求先收到怎么办
			// 在这样的情况下，就有了前面说的哪个问题。此时同处一个term，entries就是哪个多的问题
			// 不存在不一致的问题，所以只要判断哪个更长，如果arg.Entries更长就加上多余的部分
			len1, len2 := len(rf.log), len(rf.log[:pos+1])+len(args.Entries)
			if len2 > len1 {
				rf.log = append(rf.log, args.Entries[len(args.Entries)-(len2-len1):]...)
			}
			// 往applyCh发送新的committed log
			oldCommitIndex := rf.commitIndex
			rf.commitIndex = args.LeaderCommit
			// 这是figure 2里的AppendEntries RPC的receiver implementation的5
			if rf.commitIndex > rf.log[len(rf.log)-1].Index {
				rf.commitIndex = rf.log[len(rf.log)-1].Index
			}
			// 找要发送的entry的起终点并发送
			var start, end int
			for j := len(rf.log) - 1; j >= 0; j-- {
				if rf.log[j].Index == rf.commitIndex {
					end = j
				}
				if rf.log[j].Index == oldCommitIndex {
					start = j + 1
				}
			}
			for j := start; j <= end; j++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[j].Command,
					CommandIndex: rf.log[j].Index,
				}
			}

		} else {
			reply.Success = false
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
// 只有leader才会开始协商
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// 不保证Start()调用后命令是commit的
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.currentTerm, rf.role == Leader
	if isLeader {
		index = rf.log[len(rf.log)-1].Index + 1
		rf.log = append(rf.log, Entry{
			Term:    rf.currentTerm,
			Index:   index,
			Command: command,
		})
		go rf.heartbeat()
	}
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
		ms := 500 + (rand.Int63() % 150)
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
	// 写part2b时有了思考，认为这里仍然是有bug的，没有考虑发起rpc前后的rf.currentTerm变化的可能性
	// 也没有考虑各个rpc发出的状态信息都一致，所以这里也修改为和heartbeat一样的思路
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	lastLogIndex := rf.log[len(rf.log)-1].Index
	lastLogTerm := rf.log[len(rf.log)-1].Term
	rf.role = Candidate
	rf.votedFor = rf.me
	numVote := 1 // 自己一票
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
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
					// 检查更新term
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1

						// 这么判断和理由和heartbeat里一样
					} else if reply.Term == rf.currentTerm && reply.Term == currentTerm {
						if reply.VoteGranted {
							numVote++
							// 判断是否能转变为leader，如果能就转变
							if rf.role == Candidate && numVote > len(rf.peers)/2 {
								rf.role = Leader
								rf.nextIndex = make([]int, len(rf.peers))
								rf.matchIndex = make([]int, len(rf.peers))
								for i := range rf.nextIndex {
									rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
									rf.matchIndex[i] = 0
								}
								go rf.heartbeatTicker()
							}
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
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

		ms := 40 // 题目限制一秒最多几十次的数量级
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 发出heartbeat，这里heartbeat不理解为没有entries，而是也包括添加新条目后发起的协商
func (rf *Raft) heartbeat() {
	// 先加锁，因为后面可能修改rf状态
	// 之后看看别人怎么实现
	// heartbeat通信时，应该总是以当时的状态，否则在重发时有些状态实时更新不符合逻辑
	rf.mu.Lock()
	isLeader := rf.role == Leader
	numReplica := 1 // 用来判断leader发起这次heartbeat时的最后一条entry是否committed
	currentTerm := rf.currentTerm
	posAfterLastLog := len(rf.log)
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	if !isLeader {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				over := false
				for !over {
					rf.mu.Lock()
					prevLogIndex := rf.nextIndex[i] - 1
					var prevLogTerm int
					var prevLogPos int
					for j := len(rf.log) - 1; j >= 0; j-- {
						if rf.log[j].Index == prevLogIndex {
							prevLogTerm = rf.log[j].Term
							prevLogPos = j
							break
						}
					}
					// 如果后续添加条目后的heartbeat已经更改了状态，则本次heartbeat没有必要了，必然已复制
					if prevLogPos+1 > posAfterLastLog {
						numReplica++
						rf.mu.Unlock()
						over = true
						continue
					}
					// 貌似空entry的情况不用单独区分
					entries := rf.log[prevLogPos+1 : posAfterLastLog]
					rf.mu.Unlock()

					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: commitIndex,
					}
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, &args, &reply)
					if ok {
						rf.mu.Lock()
						// 这是每次RPC都必要的term检查更新
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.role = Follower
							rf.votedFor = -1

							over = true

							// reply.Term < rf.currentTerm就直接丢弃回复，
							// 但reply.Term == rf.curretnTerm还不等同于reply.Term == currentTerm，
							// 总是有reply.Term >= currentTerm。有可能大于，此时直接不处理就行
						} else if reply.Term == rf.currentTerm && reply.Term == currentTerm {
							if reply.Success {
								rf.nextIndex[i] = rf.log[posAfterLastLog-1].Index + 1
								rf.matchIndex[i] = rf.log[posAfterLastLog-1].Index
								numReplica++
								// 到这里一系列的条件判断保证了目前还是发起rpc前的状态，仍然是Leader
								// 这时就可以检查是否到posAfterLastLog-1的entry都能变为committed
								if numReplica > len(rf.peers)/2 && rf.commitIndex < rf.log[posAfterLastLog-1].Index {
									// 新的committed的log要传入rf.applyCh
									oldCommitIndex := rf.commitIndex
									rf.commitIndex = rf.log[posAfterLastLog-1].Index

									for j := posAfterLastLog - 1; j >= 0; j-- {
										if rf.log[j].Index == oldCommitIndex {
											for k := j + 1; k < posAfterLastLog; k++ {
												rf.applyCh <- ApplyMsg{
													CommandValid: true,
													Command:      rf.log[k].Command,
													CommandIndex: rf.log[k].Index,
												}
											}
											break
										}
									}
								}
								over = true
							} else {
								// 此时就递减nextIndex再重发
								rf.nextIndex[i]--
							}
						} else {
							over = true
						}
						rf.mu.Unlock()
					} else {
						over = true
					}
				}
			}(i)
		}
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

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Entry{{Index: 0, Term: -1}}
	rf.commitIndex = 0

	rf.role = Follower
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
