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
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType                 opType
	Key, Value             string
	ClientId, SerialNumber int
	ConfigNum              int
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
	dead        int32 // set by Kill()
	data        map[string]string
	lastApplied int
	lastResult  map[int]result
	cond        sync.Cond
	persister   *raft.Persister
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
	// 先判断是否负责请求的key所在shard以及配置是否改变。
	// 如果先判断是否leader，则会导致客户那边for循环中可能多作了无意义的rpc调用
	// 注意，我这里判断configNum，既可能是客户的config比较旧，因为从发送到收到期间config更新；
	// 也有可能服务器的config比较旧，因为我只定期查询config，可能更新了还没查询
	// 但为了一开始实现简单，我就直接让客户重发请求。这是个可以优化的地方
	if args.ConfigNum != kv.config.Num {
		reply.Err = ErrConfigNotMatch
		return
	}
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		kv.initIfNotExist(args.ClientId)

		if kv.lastResult[args.ClientId].SerialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
		} else if kv.lastResult[args.ClientId].SerialNumber == args.SerialNumber {
			reply.Err = OK
			if args.OpType == getOp {
				reply.Value = kv.lastResult[args.ClientId].Value
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
				if kv.config.Shards[key2shard(args.Key)] != kv.gid {
					reply.Err = ErrWrongGroup
					return
				}
				kv.initIfNotExist(args.ClientId)
				newTerm, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.Err = ErrWrongLeader
				} else if newTerm > oldTerm || kv.lastResult[args.ClientId].SerialNumber != args.SerialNumber {
					reply.Err = ErrCommitProbablyFail
				} else {
					reply.Err = OK
					if args.OpType == getOp {
						reply.Value = kv.lastResult[args.ClientId].Value
					}
				}
			}
		}
	}
}

func (kv *ShardKV) initIfNotExist(clientId int) {
	_, ok := kv.lastResult[clientId]
	if !ok {
		kv.lastResult[clientId] = result{
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
			kv.initIfNotExist(command.ClientId)
			if kv.lastResult[command.ClientId].SerialNumber < command.SerialNumber {
				switch command.OpType {
				case getOp:
					kv.lastResult[command.ClientId] = result{
						Value:        kv.data[command.Key],
						SerialNumber: command.SerialNumber,
					}
				case putOp:
					kv.lastResult[command.ClientId] = result{
						SerialNumber: command.SerialNumber,
					}
					kv.data[command.Key] = command.Value
				case appendOp:
					kv.lastResult[command.ClientId] = result{
						SerialNumber: command.SerialNumber,
					}
					_, ok := kv.data[command.Key]
					if ok {
						kv.data[command.Key] += command.Value
					} else {
						kv.data[command.Key] = command.Value
					}
				default:
					// 会到这里说明有错
					panic("出现未定义的操作\n")
				}
			}
		} else if msg.SnapshotValid {
			// 从快照恢复状态
			snapshot := msg.Snapshot
			buf := bytes.NewBuffer(snapshot)
			dec := labgob.NewDecoder(buf)
			var data map[string]string
			var lastResult map[int]result
			if dec.Decode(&data) != nil ||
				dec.Decode(&lastResult) != nil {
				panic("快照的信息不全\n")
			} else {
				kv.data = data
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
	enc.Encode(kv.lastResult)
	snapshot := buf.Bytes()
	kv.rf.Snapshot(kv.lastApplied, snapshot)
}

// 定时查询配置变化
func (kv *ShardKV) periodicallyQuery() {
	// 先不实现配置变动相关的，也就是第一个测试只有一次从初始状态到有效配置的
	// 要实现配置变动相关的，这里每次要检查新的查询结果和原来的是否一样，
	// 不一样要触发配置变动相关的操作
	for !kv.killed() {

		cf := kv.mck.Query(-1)
		kv.mu.Lock()
		kv.config = cf
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
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
	kv.data = make(map[string]string)
	kv.lastApplied = 0
	kv.lastResult = make(map[int]result)
	kv.cond = *sync.NewCond(&kv.mu)
	kv.persister = persister
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.config = kv.mck.Query(-1)

	go kv.applier()
	go kv.ticker()
	go kv.periodicallyQuery()

	return kv
}
