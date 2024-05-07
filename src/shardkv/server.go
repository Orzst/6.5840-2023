package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type opType int

const (
	undefinedOp opType = iota
	getOp
	putOp
	appendOp
	reconfigure
	shardSent
	shardReceived
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 共用
	OpType    opType
	ConfigNum int // 表示Op希望发生的作用所对应的configNum
	// get, put, append用
	Key, Value             string
	ClientId, SerialNumber int
	// reconfigure用
	Config shardctrler.Config
	// shardSent和shardReceived用
	ShardNum int
	// shardReceived用
	Shard map[string]string
}

type lab4bRPCReply struct {
	// 共用
	Err Err
	// Get需要
	Value string
}

type result struct {
	Value        string
	SerialNumber int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead          int32 // set by Kill()
	data          [shardctrler.NShards]map[string]string
	shardPrepared [shardctrler.NShards]bool // 新分配到的还没接收的，以及要发送的，就是false；其他情况为true
	lastApplied   int
	lastResult    [shardctrler.NShards]map[int]result
	cond          sync.Cond
	persister     *raft.Persister
	// sharding相关的
	mck    *shardctrler.Clerk
	config shardctrler.Config
}

// get, put, append的处理整体思路和lab3一致，但与lab4a一样对代码结构做一些调整，减少重复代码
// 沿用lab3和lab4a的代码的注释已删除，留下的主要是针对lab4b的注释

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	wrappedArgs := Op{
		OpType:       getOp,
		Key:          args.Key,
		ClientId:     args.ClientId,
		SerialNumber: args.SerialNumber,
		ConfigNum:    args.ConfigNum,
	}
	wrappedReply := lab4bRPCReply{}
	kv.rpcProcessL(&wrappedArgs, &wrappedReply)
	reply.Err = wrappedReply.Err
	reply.Value = wrappedReply.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var t opType
	if args.Op == "Put" {
		t = putOp
	} else if args.Op == "Append" {
		t = appendOp
	} else {
		// 不应该到这里
		panic("既不是put也不是append")
	}

	wrappedArgs := Op{
		OpType:       t,
		Key:          args.Key,
		Value:        args.Value,
		ClientId:     args.ClientId,
		SerialNumber: args.SerialNumber,
		ConfigNum:    args.ConfigNum,
	}
	wrappedReply := lab4bRPCReply{}
	kv.rpcProcessL(&wrappedArgs, &wrappedReply)
	reply.Err = wrappedReply.Err
}

// 该函数假定调用者已持有kv.mu
func (kv *ShardKV) rpcProcessL(args *Op, reply *lab4bRPCReply) {
	shardNum := key2shard(args.Key)
	// 如果shard需要传输过来，要等到传输完成。当然我这里退出不一定只发生了传输完成
	// 等待期间可能不仅这次传输完成了，还发生了更多变化，不过反正都会在后续判断
	for !kv.shardPrepared[shardNum] {
		kv.cond.Wait()
	}
	// 先判断是否负责请求的key所在shard以及配置是否改变。
	// 如果先判断是否leader，则会导致客户那边for循环中可能多作了无意义的rpc调用
	// 注意，我这里判断configNum，既可能是客户的config比较旧，因为从发送到收到期间，以及上面等待期间config更新；
	// 也有可能服务器的config比较旧，因为我只定期查询config，可能更新了还没查询
	// 但为了一开始实现简单，我就直接让客户重发请求。这是个可以优化的地方
	if args.ConfigNum != kv.config.Num {
		reply.Err = ErrConfigNotMatch
		return
	}
	if kv.config.Shards[shardNum] != kv.gid {
		// 这个判断可以不要，因为configNum一样的话，这个必然满足，否则客户也不会发过来
		reply.Err = ErrWrongGroup
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		kv.initIfNotExist(args.ClientId, shardNum)

		if kv.lastResult[shardNum][args.ClientId].SerialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
		} else if kv.lastResult[shardNum][args.ClientId].SerialNumber == args.SerialNumber {
			reply.Err = OK
			if args.OpType == getOp {
				reply.Value = kv.lastResult[shardNum][args.ClientId].Value
			}
		} else {
			index, oldTerm, isLeader := kv.rf.Start(*args)
			if !isLeader {
				reply.Err = ErrWrongLeader
			} else {
				var newTerm int
				for index > kv.lastApplied {
					newTerm, isLeader = kv.rf.GetState()
					if !isLeader || newTerm > oldTerm {
						break
					}
					kv.cond.Wait()
				}
				// 由于中间释放了锁，所以状态可能发生了改变，各种都要再次判断，包括sharding配置的改变
				// 这里和开头是一样的判断。只要操作由raft确认commit，即使这里响应为错误，
				// 只要客户重发请求，最终一定能有一台服务器作出合适的响应。
				if args.ConfigNum != kv.config.Num {
					reply.Err = ErrConfigNotMatch
					return
				}
				if kv.config.Shards[shardNum] != kv.gid {
					// 这个和开头一样，其实可以不要
					reply.Err = ErrWrongGroup
					return
				}
				kv.initIfNotExist(args.ClientId, shardNum)
				newTerm, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.Err = ErrWrongLeader
				} else if newTerm > oldTerm || kv.lastResult[shardNum][args.ClientId].SerialNumber != args.SerialNumber {
					reply.Err = ErrCommitProbablyFail
				} else {
					reply.Err = OK
					if args.OpType == getOp {
						reply.Value = kv.lastResult[shardNum][args.ClientId].Value
					}
				}
			}
		}
	}
}

func (kv *ShardKV) initIfNotExist(clientId, shardNum int) {
	_, ok := kv.lastResult[shardNum][clientId]
	if !ok {
		kv.lastResult[shardNum][clientId] = result{
			SerialNumber: -1,
		}
	}
}

func (kv *ShardKV) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.mu.Lock()
		if msg.CommandValid {
			kv.lastApplied = msg.CommandIndex
			command := msg.Command.(Op)
			shardNum := key2shard(command.Key)
			kv.initIfNotExist(command.ClientId, shardNum)
			switch command.OpType {
			case getOp, putOp, appendOp:
				// 引入sharding后这里条件要添加，因为提交log时可能的确是这个组负责shard，但是apply
				// 时可能已经不再是，此时就不能进行apply
				if command.ConfigNum == kv.config.Num &&
					kv.lastResult[shardNum][command.ClientId].SerialNumber < command.SerialNumber {
					switch command.OpType {
					case getOp:
						kv.lastResult[shardNum][command.ClientId] = result{
							Value:        kv.data[shardNum][command.Key],
							SerialNumber: command.SerialNumber,
						}
					case putOp:
						kv.lastResult[shardNum][command.ClientId] = result{
							SerialNumber: command.SerialNumber,
						}
						kv.data[shardNum][command.Key] = command.Value
					case appendOp:
						kv.lastResult[shardNum][command.ClientId] = result{
							SerialNumber: command.SerialNumber,
						}
						_, ok := kv.data[shardNum][command.Key]
						if ok {
							kv.data[shardNum][command.Key] += command.Value
						} else {
							kv.data[shardNum][command.Key] = command.Value
						}
					}
				}
			case reconfigure:
				// 也要去重。此时其实就是command.ConfigNum == kv.config.Num
				if command.Config.Num == kv.config.Num+1 {
					for i := range kv.shardPrepared {
						if kv.config.Shards[i] != kv.gid && command.Config.Shards[i] != kv.gid ||
							kv.config.Shards[i] == command.Config.Shards[i] ||
							kv.config.Shards[i] == 0 || command.Config.Shards[i] == 0 {
							// 更新前后都跟这个组没关系，或者有关系但没变化。
							// 还有别的情况一开始忘了考虑：如果gid原来是0（从初始状态变化过来），则不需要从哪里接收，也是true
							// 如果变成了0，就是shard不要了（这种情况lab里应该没有），也算true
							kv.shardPrepared[i] = true
						} else {
							// 更新前是这个组的，更新后不是了，要发送
							// 更新前不是这个组的，更新后是了，要接收
							kv.shardPrepared[i] = false
							// 发送相关的操作在另外的线程，通过检查这个设置的状态来触发，
							// 接收相关的操作则在发送调用的rpc接口里进行。两者成功时都会
							// 提交raft日志，注意考虑apply时是否需要去重
						}
					}
				}
				kv.config = command.Config
			case shardSent:
				// 要考虑config变动没有。可以不考虑是否已经设置为true，因为反复设置没什么影响
				if command.ConfigNum == kv.config.Num {
					kv.shardPrepared[command.ShardNum] = true
				}
			case shardReceived:
				// 这个要注意去重，即使是当前config，但接收shard意味着这个shard是当前组负责的，
				// 所以可能发生了修改，不去重可能会回退到旧的状态
				if command.ConfigNum == kv.config.Num && !kv.shardPrepared[command.ShardNum] {
					kv.shardPrepared[command.ShardNum] = true
					// 这里要将command.Shard的内容复制一份，否则因为map是引用，会与raft发生数据竞态
					// 这是lab的一个提示，自己感觉挺难注意到这个细节
					copiedShard := make(map[string]string, len(command.Shard))
					for k, v := range command.Shard {
						copiedShard[k] = v
					}
					kv.data[command.ShardNum] = copiedShard
				}
			default:
				// 会到这里说明有错
				panic("出现未定义的操作\n")
			}
		} else if msg.SnapshotValid {
			// 从快照恢复状态
			snapshot := msg.Snapshot
			buf := bytes.NewBuffer(snapshot)
			dec := labgob.NewDecoder(buf)
			var data [shardctrler.NShards]map[string]string
			var shardPrepared [shardctrler.NShards]bool
			var lastResult [shardctrler.NShards]map[int]result
			if dec.Decode(&data) != nil ||
				dec.Decode(&shardPrepared) != nil ||
				dec.Decode(&lastResult) != nil {
				panic("快照的信息不全\n")
			} else {
				kv.data = data
				kv.shardPrepared = shardPrepared
				kv.lastResult = lastResult
			}
			kv.lastApplied = msg.SnapshotIndex
		} else {
			// 不应该会走到这里
			panic("msg.CommandValid和msg.SnapshotValid都是false\n")
		}
		kv.cond.Broadcast()
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)

		kv.mu.Lock()
		kv.cond.Broadcast()
		if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
			kv.takeSnapshot()
		}
		kv.mu.Unlock()
	}
}

// 该函数假定调用时已持有锁
func (kv *ShardKV) takeSnapshot() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(kv.data)
	enc.Encode(kv.shardPrepared) // lab4b新增的
	enc.Encode(kv.lastResult)
	snapshot := buf.Bytes()
	kv.rf.Snapshot(kv.lastApplied, snapshot)
}

// 定时查询配置变化
func (kv *ShardKV) periodicallyQuery() {
	// 涉及配置变动后，这里每次查的不应该是最新的，而应该是当前配置的下一个
	// 而且只有当前配置需要发送和接收的shard都发送接收完成后才查下一个
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			allShardsPrepared := true
			for _, t := range kv.shardPrepared {
				allShardsPrepared = allShardsPrepared && t
			}
			configNum := kv.config.Num
			kv.mu.Unlock()

			if allShardsPrepared {
				cf := kv.mck.Query(configNum + 1)

				kv.mu.Lock()
				if cf.Num == kv.config.Num+1 {
					// 需要更新，更新操作要通过raft来保持一致，所以具体操作在apply时进行
					op := Op{
						OpType:    reconfigure,
						ConfigNum: kv.config.Num,
						Config:    cf,
					}
					// 这里仅尝试提交给raft，下次循环到这里时什么状态都不重要
					// 如果已经apply，那就是查询下一个配置，这就不需要什么特别处理；
					// 如果已经commit但还没apply，或者还没commit但最终会commit，
					// 就会提交一个重复的，这种情况要注意applier里去重，通过configNum判断就行；
					// 如果最后无法commit，或者提交时就不是leader而失败，则就是需要循环的。
					kv.rf.Start(op)
				}
				// 其他的情况不需要操作
				kv.mu.Unlock()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) shardSender() {
	for !kv.killed() {
		// 只有Leader负责发送shard
		_, isLeader := kv.rf.GetState()
		if isLeader {
			// 检查有没有需要发送的，有的话就不断尝试发送直到成功
			kv.mu.Lock()
			var wg sync.WaitGroup
			for i := range kv.shardPrepared {
				if !kv.shardPrepared[i] && kv.config.Shards[i] != kv.gid {
					// 在另一个goroutine中发送，避免死锁
					copiedShard := make(map[string]string)
					for k, v := range kv.data[i] {
						copiedShard[k] = v
					}
					wg.Add(1)
					go func(shardNum, configNum int, shard map[string]string) {
						// 发送shard相关的rpc调用
						args := TransferShardArgs{
							ShardNum:  shardNum,
							Shard:     shard,
							ConfigNum: configNum,
						}
						for _, s := range kv.config.Groups[kv.config.Shards[shardNum]] {
							srv := kv.make_end(s)
							reply := TransferShardReply{}
							ok := srv.Call("ShardKV.TransferShard", &args, &reply)
							if ok && reply.Success {
								// 被成功接收则要尝试提交相关的raft log，注意判断是否还是发送时的状态
								kv.mu.Lock()
								if kv.config.Num == configNum && !kv.shardPrepared[shardNum] {
									op := Op{
										OpType:    shardSent,
										ShardNum:  shardNum,
										ConfigNum: configNum,
									}
									kv.rf.Start(op)
								}
								kv.mu.Unlock()
							}
						}
						wg.Done()
					}(i, kv.config.Num, copiedShard)
				}
			}
			kv.mu.Unlock()
			// 有发送的话，一轮发送结束了，再尝试下一轮，避免不必要的重复发送
			wg.Wait()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 定义一下发送和接收shard相关的rpc操作

type TransferShardArgs struct {
	ShardNum int
	Shard    map[string]string
	// 用于处理过时请求。如果接收者的configNum更大，由于我的设计中只有一个config的
	// 传输都完成了，才有可能推进到下一个config，所以此时就可以响应说成功
	// 如果接收者的configNum更小，表示还没更新到那一步，就响应说失败
	ConfigNum int
}

type TransferShardReply struct {
	// 注意，成功还是失败是看raft的相应条目是否apply，而不是是否接收到rpc请求
	// 类似lab里客户给服务器发的请求的成功与否的判断
	Success bool
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 先判断configNum与接收者不同的情况
	if args.ConfigNum < kv.config.Num {
		reply.Success = true
		return
	}
	if args.ConfigNum > kv.config.Num {
		reply.Success = false
		return
	}
	// 相同的再判断是否已经成功过了
	if kv.shardPrepared[args.ShardNum] {
		reply.Success = true
		return
	}
	// 到这里才是真的需要接收传输的shard内容
	// 只有leader负责接收，因为所谓接收就是尝试提交raft log，实际状态改变都是通过raft的apply实现的
	op := Op{
		OpType:    shardReceived,
		ConfigNum: args.ConfigNum,
		ShardNum:  args.ShardNum,
		Shard:     args.Shard,
	}
	index, oldTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		return
	}
	// 作为leader提交了，那么就要等待看apply是否成功
	var newTerm int
	for index > kv.lastApplied {
		newTerm, isLeader = kv.rf.GetState()
		if !isLeader || newTerm > oldTerm {
			break
		}
		kv.cond.Wait()
	}
	newTerm, isLeader = kv.rf.GetState()
	if !isLeader || newTerm > oldTerm || !kv.shardPrepared[args.ShardNum] {
		// 这些是要发送者找leader去确认，或者没有成功commit
		reply.Success = false
	} else {
		reply.Success = true
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end // make_end用来将Config里服务器字符串名映射为labrpc.ClientEnd
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 自己添加的字段的初始化
	for i := range kv.data {
		kv.data[i] = make(map[string]string)
		kv.shardPrepared[i] = true
		kv.lastResult[i] = make(map[int]result)
	}
	kv.lastApplied = 0
	kv.cond = *sync.NewCond(&kv.mu)
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.mck.Query(-1)

	go kv.applier()
	go kv.ticker()
	go kv.periodicallyQuery()
	go kv.shardSender()

	return kv
}
