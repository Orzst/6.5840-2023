package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

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
	Value string
	// index int // 不需要，因为比较的是serialNumber
	// clientId int //不需要，因为result是map的值，map的键就是clientId
	SerialNumber int
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
	// 这里应该是在raft.Raft里有个调用它的persister的方法得到结果的接口
	// 但这要改lab2的代码，改了得重新跑测试确认没问题，我懒得改，就直接
	// 在这里用同一个persister的指针直接调用了，有点丑陋
	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("%v\n进入kv.Get()\n", time.Now())
	// defer DPrintf("%v\n离开kv.Get()\n", time.Now())

	// 每次收到请求，都检查一下当前有没有必要生成快照
	// if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
	// 	kv.takeSnapshot()
	// }
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
				SerialNumber: -1,
			}
		}
		// 再判断是否是过时的，最新的已应用的（即重复的情况），还是最新的还没应用的请求
		if kv.lastResult[args.ClientId].SerialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
		} else if kv.lastResult[args.ClientId].SerialNumber == args.SerialNumber {
			reply.Err = OK
			reply.Value = kv.lastResult[args.ClientId].Value
			// DPrintf("%v\nserver %d 执行get(%s)，得到%s\n目前的data长度: %d\n目前的data[0]: %s\n", time.Now(), kv.me, args.Key, reply.Value, len(kv.data), kv.data["0"])
			// fmt.Printf("%v\nserver %d 执行get(%s)，得到%s\n目前的data长度: %d\n目前的data: %s\n", time.Now(), kv.me, args.Key, reply.Value, len(kv.data), kv.data)
		} else {
			// 没应用的就尝试提交给raft
			op := Op{
				OpType:       getOp,
				Key:          args.Key,
				ClientId:     args.ClientId,
				SerialNumber: args.SerialNumber,
			}
			index, oldTerm, isLeader := kv.rf.Start(op)
			// 注意这时raft可能状态已改变，要判断
			if !isLeader {
				reply.Err = ErrWrongLeader
			} else {
				// 尝试提交之后要等raft通知apply并且实际apply了才能知道结果
				var newTerm int
				for index > kv.lastApplied {
					newTerm, isLeader = kv.rf.GetState()
					// 1、
					// 必须要考虑等待过程中不再是leader的情况
					// 如果始终是leader，则之前记录的op迟早会commit，不会有问题
					// 但是如果中途失去leader身份，有可能最终一致的log最后一条的index小于这里的index
					// 那这里就会一直处于等待状态。
					// 2、
					// 另外，考虑到有这样一种情况，所有都已经一致且都apply，当前op记录后立刻
					// 失去leader身份，而新leader又没有新的entry，此时单靠apply时的唤醒
					// 还是会因为没有apply而始终等待，所以我server有一个定时唤醒的计时器goroutine
					// 3、
					// 考虑了前两点后仍然有死循环。考虑这样一种情况，一开始都一致，是leader，记录了多条
					// entry后，暂时失去leader身份，新leader有少量提交然后达成一致，然后该服务器又变为leader
					// 这一切发生在两次唤醒之间，此后该服务器始终是leader，但如果没有更多的新命令，永远到不了index
					// 解决方案是再考虑term，因为这种情况下，服务器的term一定改变了
					if !isLeader || newTerm > oldTerm {
						break
					}
					kv.cond.Wait()
				}
				// 可以确定的是，index <= kv.lastApplied时，由于lab同一客户总是串行地发出请求，
				// 所以不会有后来的请求在前面请求从没执行过的情况下就先被执行

				// 每次要读map时都要这一步检查以及必要时的初始化，其实可以写成一个函数，但我就先这样了
				// 这里是我倒数第二个有概率不通过的测试的问题所在，即引入快照后有这样一个场景：
				// 代码执行到这里index <= kv.LastApplied，但是因为收到快照而达到这个条件的，
				// 发送快照的服务器可能没有见过当前客户，所以快照保存的lastResult里是没有相应键值对的。
				// 因为我上面的逻辑，这样跳出循环时没有最新的GetState()，于是误认为仍然isLeader
				// 且newTerm == oldTerm，再因为没有键值对时读取到的是默认零值，其SerialNumber默认零值为0
				// 而我的SerialNumber刚好是从0开始，这时就错误地到了reply.Err = OK，而实际上这条指令没有执行
				// 加上这一步初始化，或者跳出循环后再次GetState()应该都能避免这个问题，我这直接都加上
				_, ok := kv.lastResult[args.ClientId]
				if !ok {
					kv.lastResult[args.ClientId] = result{
						SerialNumber: -1,
					}
				}
				newTerm, isLeader = kv.rf.GetState()
				// 此时先判断跳出循环是否因为不再是leader，然后判断之前提交的是否commit
				if !isLeader {
					reply.Err = ErrWrongLeader
				} else if newTerm > oldTerm || kv.lastResult[op.ClientId].SerialNumber != op.SerialNumber {
					// 未必成功commit，还是有可能在未来的term里被commit，但当前为了避免死循环提前退出不好判断
					reply.Err = ErrCommitProbablyFail
				} else {
					reply.Err = OK
					reply.Value = kv.lastResult[op.ClientId].Value
					// DPrintf("%v\nserver %d 执行get(%s)，得到%s\n目前的data长度: %d\n目前的data[0]: %s\n", time.Now(), kv.me, args.Key, reply.Value, len(kv.data), kv.data["0"])
					// fmt.Printf("%v\nserver %d 执行get(%s)，得到%s\n目前的data长度: %d\n目前的data: %s\n", time.Now(), kv.me, args.Key, reply.Value, len(kv.data), kv.data)
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

	// if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
	// 	kv.takeSnapshot()
	// }
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		_, ok := kv.lastResult[args.ClientId]
		if !ok {
			kv.lastResult[args.ClientId] = result{
				SerialNumber: -1,
			}
		}
		if kv.lastResult[args.ClientId].SerialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
		} else if kv.lastResult[args.ClientId].SerialNumber == args.SerialNumber {
			reply.Err = OK
			// DPrintf("%v\nserver %d 执行%s(%s, %s)，执行后data[%s] = %s\n", time.Now(), kv.me, args.Op, args.Key, args.Value, args.Key, kv.data[args.Key])
			// if args.Key == "c" {
			// 	fmt.Printf("%v\nserver %d 执行%s(%s, %s)，执行后data[\"c\"] = %s\n此时kv.lastApplied = %d\n", time.Now(), kv.me, args.Op, args.Key, args.Value, kv.data[args.Key], kv.lastApplied)
			// }
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
			index, oldTerm, isLeader := kv.rf.Start(op)
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
				_, ok := kv.lastResult[args.ClientId]
				if !ok {
					kv.lastResult[args.ClientId] = result{
						SerialNumber: -1,
					}
				}
				newTerm, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.Err = ErrWrongLeader
				} else if newTerm > oldTerm || kv.lastResult[op.ClientId].SerialNumber != op.SerialNumber {
					reply.Err = ErrCommitProbablyFail
				} else {
					reply.Err = OK
					// DPrintf("%v\nserver %d 执行%s(%s, %s)，执行后data[%s] = %s\n", time.Now(), kv.me, args.Op, args.Key, args.Value, args.Key, kv.data[args.Key])
					// if args.Key == "c" {
					// 	fmt.Printf("%v\nserver %d 执行%s(%s, %s)，执行后data[\"c\"] = %s\n此时index = %d, kv.lastApplied = %d, lastResult[%d]: \n%v\n完整的op: %v\n", time.Now(), kv.me, args.Op, args.Key, args.Value, kv.data[args.Key], index, kv.lastApplied, op.ClientId, kv.lastResult[op.ClientId], op)
					// }
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
		// if msg.CommandValid {
		// 	DPrintf("server %d 收到命令ApplyMsg, index: %d\n执行前kv.data: %v\n", kv.me, msg.CommandIndex, kv.data)
		// } else {
		// 	DPrintf("server %d 收到快照ApplyMsg, index: %d\n执行前kv.data: %v\n", kv.me, msg.SnapshotIndex, kv.data)
		// }
		if msg.CommandValid {
			kv.lastApplied = msg.CommandIndex
			command := msg.Command.(Op)
			_, ok := kv.lastResult[command.ClientId]
			if !ok {
				kv.lastResult[command.ClientId] = result{
					SerialNumber: -1,
				}
			}
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
					// if command.Key == "c" {
					// 	fmt.Printf("C相关的这条指令：%v\n", command)
					// }
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
			// DPrintf("server %d 执行了第 %d 条命令后:\nkv.data: %v\nkv.lastResult: %v\n", kv.me, kv.lastApplied, kv.data, kv.lastResult)
			// if kv.persister.RaftStateSize() > 2000 {
			// 	DPrintf("server %d 的lastApplied: %d, raftStateSize: %d\n", kv.me, kv.lastApplied, kv.persister.RaftStateSize())
			// }
			// if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
			// 	kv.takeSnapshot()
			// 	// DPrintf("生成快照后，server %d 的lastApplied: %d, raftStateSize: %d\n", kv.me, kv.lastApplied, kv.persister.RaftStateSize())
			// }
		} else if msg.SnapshotValid {
			// 从快照恢复状态
			// DPrintf("server %d 读取快照前的kv.lastApplied: %d\n", kv.me, kv.lastApplied)
			// DPrintf("server %d 读取快照前的kv.data:\n%v\nserver %[1]d 读取后的kv.lastResult:\n%v\n", kv.me, kv.data, kv.lastResult)
			snapshot := msg.Snapshot
			buf := bytes.NewBuffer(snapshot)
			dec := labgob.NewDecoder(buf)
			var data map[string]string
			var lastResult map[int]result
			if dec.Decode(&data) != nil ||
				dec.Decode(&lastResult) != nil {
				panic("快照的信息不全\n")
			} else {
				// DPrintf("server %d 读取到的data:\n%v\nserver %[1]d 读取到的lastResult:\n%v\n", kv.me, data, lastResult)
				kv.data = data
				kv.lastResult = lastResult
			}
			kv.lastApplied = msg.SnapshotIndex
			// DPrintf("server %d 读取后的kv.data:\n%v\nserver %[1]d 读取后的kv.lastResult:\n%v\n", kv.me, kv.data, kv.lastResult)
		} else {
			// 不应该会走到这里
			panic("msg.CommandValid和msg.SnapshotValid都是false\n")
		}
		kv.cond.Broadcast()
		// 引入快照之后这里还要改
		kv.mu.Unlock()
		// DPrintf("%v\nserver %d 结束一次apply\n", time.Now(), kv.me)
	}
}

func (kv *KVServer) ticker() {
	for !kv.killed() {
		time.Sleep(10 * time.Millisecond)

		kv.mu.Lock()
		kv.cond.Broadcast() // 这个是为了rpc处理中的等待的定时唤醒，防止死循环，和快照无关
		// 只在定时器中检查是否生成快照并做相应处理。这里应该是我最后一个有概率不通过的测试的解决方案
		// 问题就是之前在apply中检查，一旦raftState超过限制，且apply速度慢于增加速度，则会导致
		// 每次apply都生成快照，这种频繁的生成反而进一步拖慢了速度。证据就是我通过输出快照大小变化，
		// 观察到不通过的时候，有一个server的apply速度明显慢于其他，且log越来越大，
		// 而如果在测试前给几秒钟停顿以便apply，则能连续50次通过测试，所以我觉得问题就在于此
		if kv.maxraftstate != -1 && kv.maxraftstate < kv.persister.RaftStateSize() {
			kv.takeSnapshot()
		}
		kv.mu.Unlock()
	}
}

// 该函数假定调用时已持有锁
func (kv *KVServer) takeSnapshot() {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(kv.data)
	// enc.Encode(kv.lastApplied) //不需要，因为Snapshot()里传入后会记录，发来的ApplyMsg里的SnapshotIndex就是
	enc.Encode(kv.lastResult)
	snapshot := buf.Bytes()
	kv.rf.Snapshot(kv.lastApplied, snapshot)
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
	kv.persister = persister

	go kv.applier()
	go kv.ticker()

	// You may need initialization code here.

	return kv
}
