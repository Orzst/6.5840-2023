package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

type opType int

const (
	undefinedOp opType = iota
	getOp
	putOp
	appendOp
	// getClientAndLeaderIdOp
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType                 opType
	Key, Value             string
	ClientId, SerialNumber int
}

// 因为只有get, put, append操作，所以只需要有一个value字段来保存结果
type result struct {
	value string
	// index int // 不需要，因为比较的是serialNumber
	// clientId int //不需要，因为result是map的值，map的键就是clientId
	serialNumber int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	// nextClientId int
	lastApplied int
	lastResult  map[int]result
	cond        sync.Cond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("%v\n进入kv.Get()\n", time.Now())
	// defer DPrintf("%v\n离开kv.Get()\n", time.Now())

	_, isLeader := kv.rf.GetState()
	// 先判断是否是Leader
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		// 注意客户第一次连接的情况，如果直接用map[key]，不存在的会返回默认的零值
		// 在这里默认的result零值里，serialNumber默认是0，这导致第一条命令会被误判为
		// 已经执行，最终导致测试不过，这个问题让我找了好一会
		_, ok := kv.lastResult[args.ClientId]
		if !ok {
			kv.lastResult[args.ClientId] = result{
				serialNumber: -1,
			}
		}
		// 再判断是否是过时的，最新的已应用的（即重复的情况），还是最新的还没应用的请求
		if kv.lastResult[args.ClientId].serialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
		} else if kv.lastResult[args.ClientId].serialNumber == args.SerialNumber {
			reply.Err = OK
			reply.Value = kv.lastResult[args.ClientId].value
			// DPrintf("%v\nserver %d 执行get(%s)，得到%s\n目前的data长度: %d\n目前的data[0]: %s\n", time.Now(), kv.me, args.Key, reply.Value, len(kv.data), kv.data["0"])
		} else {
			// 没应用的就尝试提交给raft
			op := Op{
				OpType:       getOp,
				Key:          args.Key,
				ClientId:     args.ClientId,
				SerialNumber: args.SerialNumber,
			}
			index, _, isLeader := kv.rf.Start(op)
			// 注意这时raft可能状态已改变，要判断
			if !isLeader {
				reply.Err = ErrWrongLeader
			} else {
				// 尝试提交之后要等raft通知apply并且实际apply了才能知道结果
				for index > kv.lastApplied {
					kv.cond.Wait()
				}
				// 设计的是每应用一条都会broad唤醒一次，但注意到这里未必是 index == kv.LastApplied
				// 因为锁的获取是一种竞争，有可能applier再次竞争到了
				// 但可以确定的是，由于lab同一服务器总是串行地发出请求，所以不会有后来的请求在
				// 前面请求从没执行过的情况下就先被执行

				// 此时能判断之前提交的是否commit
				if kv.lastResult[op.ClientId].serialNumber != op.SerialNumber {
					// 未能成功commit
					reply.Err = ErrCommitFail
				} else {
					reply.Err = OK
					reply.Value = kv.lastResult[op.ClientId].value
					// DPrintf("%v\nserver %d 执行get(%s)，得到%s\n目前的data长度: %d\n目前的data[0]: %s\n", time.Now(), kv.me, args.Key, reply.Value, len(kv.data), kv.data["0"])
				}
			}
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 逻辑和Get里的类似
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("%v\n进入kv.PutAppend()\n", time.Now())
	// defer DPrintf("%v\n离开kv.PutAppend()\n", time.Now())

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		_, ok := kv.lastResult[args.ClientId]
		if !ok {
			kv.lastResult[args.ClientId] = result{
				serialNumber: -1,
			}
		}
		if kv.lastResult[args.ClientId].serialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
		} else if kv.lastResult[args.ClientId].serialNumber == args.SerialNumber {
			reply.Err = OK
			// DPrintf("%v\nserver %d 执行%s(%s, %s)，执行后data[%s] = %s\n", time.Now(), kv.me, args.Op, args.Key, args.Value, args.Key, kv.data[args.Key])
		} else {
			var t opType
			if args.Op == "Put" {
				t = putOp
			} else if args.Op == "Append" {
				t = appendOp
			} else {
				t = undefinedOp // 方便暴露出bug
			}
			op := Op{
				OpType:       t,
				Key:          args.Key,
				Value:        args.Value,
				ClientId:     args.ClientId,
				SerialNumber: args.SerialNumber,
			}
			index, _, isLeader := kv.rf.Start(op)
			// 注意这时raft可能状态已改变，要判断
			if !isLeader {
				reply.Err = ErrWrongLeader
			} else {
				// 尝试提交之后要等raft通知apply并且实际apply了才能知道结果
				for index > kv.lastApplied {
					kv.cond.Wait()
				}
				// 设计的是每应用一条都会broad唤醒一次，但注意到这里未必是 index == kv.LastApplied
				// 因为锁的获取是一种竞争，有可能applier再次竞争到了
				// 但可以确定的是，由于lab同一服务器总是串行地发出请求，所以不会有后来的请求在
				// 前面请求从没执行过的情况下就先被执行

				// 此时能判断之前提交的是否commit
				if kv.lastResult[op.ClientId].serialNumber != op.SerialNumber {
					// 未能成功commit
					reply.Err = ErrCommitFail
				} else {
					reply.Err = OK
					// DPrintf("%v\nserver %d 执行%s(%s, %s)，执行后data[%s] = %s\n", time.Now(), kv.me, args.Op, args.Key, args.Value, args.Key, kv.data[args.Key])
				}
			}
		}
	}
}

// // 自己添加一个获取clientId的rpc调用，顺便也能获取一开始的leaderId。暂时不实现这个方案了
// func (kv *KVServer) GetClientIdAndLeaderId(args *GetClientIdAndLeaderIdArgs, reply *GetClientIdAndLeaderIdReply) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	_, isLeader := kv.rf.GetState()
// 	if isLeader {
// 		reply.ClientId = kv.nextClientId
// 		kv.nextClientId++
// 		reply.LeaderId = kv.me
// 	} else {
// 		reply.LeaderId = -1
// 	}
// }

func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}
		// DPrintf("%v\nserver %d 开始一次apply\n", time.Now(), kv.me)
		kv.mu.Lock()
		// var buf bytes.Buffer
		// for k, v := range kv.data {
		// 	buf.WriteString(fmt.Sprintf("data[%s] = %s\n", k, v))
		// }
		// DPrintf("server %d 进行了一次apply\n目前的数据：\n%s", kv.me, buf.String())
		if msg.CommandValid {
			kv.lastApplied = msg.CommandIndex
			command := msg.Command.(Op)
			switch command.OpType {
			case getOp:
				kv.lastResult[command.ClientId] = result{
					value:        kv.data[command.Key],
					serialNumber: command.SerialNumber,
				}
			case putOp:
				kv.lastResult[command.ClientId] = result{
					serialNumber: command.SerialNumber,
				}
				kv.data[command.Key] = command.Value
			case appendOp:
				kv.lastResult[command.ClientId] = result{
					serialNumber: command.SerialNumber,
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
		kv.cond.Broadcast()
		// 引入快照之后这里还要改
		kv.mu.Unlock()
		// DPrintf("%v\nserver %d 结束一次apply\n", time.Now(), kv.me)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers里相当于是标识了每一个服务器
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// me是当前服务器在servers的索引
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// 在raft的状态超过maxraftstate指定的byte数量时要求保存快照并要求raft根据快照压缩
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// maxraftstate为-1表示不使用快照，即永不压缩，lab3A里先不涉及快照，可以设为-1
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	// kv.nextClientId = 0 // 暂时简单实现为client序号一直递增，lab够用了
	kv.lastApplied = 0
	kv.lastResult = make(map[int]result)
	kv.cond = *sync.NewCond(&kv.mu)

	go kv.applier()

	// You may need initialization code here.

	return kv
}
