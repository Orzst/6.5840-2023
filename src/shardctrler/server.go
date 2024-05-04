package shardctrler

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

// controller这里raft部分不要求用到快照

// 几个RPC的reply内容基本一致，就不用interface{}了，直接包含可能需要的字段
type result struct {
	SerialNumber int
	Config       Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	dead        int32 // set by Kill()
	lastApplied int
	lastResult  map[int]result
	cond        sync.Cond
}

type opType int

const (
	undefinedOp opType = iota
	joinOp
	leaveOp
	moveOp
	queryOp
)

// 为了减少重复代码，定义了的类包含所有属性

type Op struct {
	// Your data here.
	// 共用
	OpType       opType
	ClientId     int
	SerialNumber int
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
}

type Lab4aRPCReply struct {
	// 共用
	WrongLeader bool
	Err         Err
	// Query
	Config Config
}

func (sc *ShardCtrler) initIfNotExist(clientId int) {
	_, ok := sc.lastResult[clientId]
	if !ok {
		sc.lastResult[clientId] = result{
			SerialNumber: -1,
		}
	}
}

// 几个RPC的处理有很多共同的逻辑，
// 因为类型不同，要避免重复代码的话，考虑给args和reply
// 分别定义接口，接口有getter和setter来获取和设置代码里
// 涉及的字段，这样的话应该能避免重复代码
// 另一个简单点的做法就是定义一个结构包含所有args可能用到的属性
// 一个结构包含所有reply可能用到的属性。这样的做法思路和接口的做法
// 一致。区别在于是想直接访问属性，代码耦合度较高，而且每个实例
// 都会额外占用一点空间，但好处是代码写起来会更短。
// 我选择后者，因为少写不少代码。接口的话每个都要实现所有方法，哪怕是空
// 的内容也得写一下，看着很长，对于lab而言感觉没必要

// 调用该函数的几个RPC接口函数要自行持有锁
func (sc *ShardCtrler) rpcProcessL(args *Op, reply *Lab4aRPCReply) {
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		// lab4a中wrongLeader不再使用Err而是一个bool字段
		// 但反正Err字段还是要用来判断是否为OK的
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	} else {
		sc.initIfNotExist(args.ClientId)

		if sc.lastResult[args.ClientId].SerialNumber > args.SerialNumber {
			reply.Err = ErrOutdatedRequest
			reply.WrongLeader = false
		} else if sc.lastResult[args.ClientId].SerialNumber == args.SerialNumber {
			reply.Err = OK
			reply.WrongLeader = false
			if args.OpType == queryOp {
				reply.Config = sc.lastResult[args.ClientId].Config
			}
		} else {
			index, oldTerm, isLeader := sc.rf.Start(*args) // 注意这里*args，因为StartServer()里是Register(Op{})
			if !isLeader {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
			} else {
				var newTerm int
				for index > sc.lastApplied {
					newTerm, isLeader = sc.rf.GetState()
					if !isLeader || newTerm > oldTerm {
						break
					}
					sc.cond.Wait()
				}
				sc.initIfNotExist(args.ClientId)
				newTerm, isLeader = sc.rf.GetState()
				if !isLeader {
					reply.Err = ErrWrongLeader
					reply.WrongLeader = true
				} else if newTerm > oldTerm || sc.lastResult[args.ClientId].SerialNumber != args.SerialNumber {
					reply.Err = ErrCommitProbablyFail
					reply.WrongLeader = false
				} else {
					reply.Err = OK
					reply.WrongLeader = false
					if args.OpType == queryOp {
						reply.Config = sc.lastResult[args.ClientId].Config
					}
				}
			}
		}
	}

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrappedArgs := Op{
		OpType:       joinOp,
		ClientId:     args.ClientId,
		SerialNumber: args.SerialNumber,

		Servers: args.Servers,
	}
	wrappedReply := Lab4aRPCReply{}
	sc.rpcProcessL(&wrappedArgs, &wrappedReply)
	reply.WrongLeader = wrappedReply.WrongLeader
	reply.Err = wrappedReply.Err
}

// 下面几个的逻辑和Join差不多。实际的不同处理还是在applier里

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrappedArgs := Op{
		OpType:       leaveOp,
		ClientId:     args.ClientId,
		SerialNumber: args.SerialNumber,

		GIDs: args.GIDs,
	}
	wrappedReply := Lab4aRPCReply{}
	sc.rpcProcessL(&wrappedArgs, &wrappedReply)
	reply.WrongLeader = wrappedReply.WrongLeader
	reply.Err = wrappedReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrappedArgs := Op{
		OpType:       moveOp,
		ClientId:     args.ClientId,
		SerialNumber: args.SerialNumber,

		GID:   args.GID,
		Shard: args.Shard,
	}
	wrappedReply := Lab4aRPCReply{}
	sc.rpcProcessL(&wrappedArgs, &wrappedReply)
	reply.WrongLeader = wrappedReply.WrongLeader
	reply.Err = wrappedReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	wrappedArgs := Op{
		OpType:       queryOp,
		ClientId:     args.ClientId,
		SerialNumber: args.SerialNumber,

		Num: args.Num,
	}
	wrappedReply := Lab4aRPCReply{}
	sc.rpcProcessL(&wrappedArgs, &wrappedReply)
	reply.WrongLeader = wrappedReply.WrongLeader
	reply.Err = wrappedReply.Err
	reply.Config = wrappedReply.Config
}

// lab4a没有提供这个killed()，我觉得有需要就自己复制过来了
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) ticker() {
	for !sc.killed() {
		time.Sleep(10 * time.Millisecond)
		sc.cond.Broadcast()
	}
}

func (sc *ShardCtrler) applier() {
	for msg := range sc.applyCh {
		if sc.killed() {
			break
		}
		sc.mu.Lock()
		if msg.CommandValid {
			sc.lastApplied = msg.CommandIndex
			command := msg.Command.(Op)
			sc.initIfNotExist(command.ClientId)
			if sc.lastResult[command.ClientId].SerialNumber < command.SerialNumber {
				switch command.OpType {
				// Join和Leave涉及的重新给shard分配gid的逻辑大部分其实是一致的
				// 因为由于Move的存在，发生Join和Leave都可能同时存在原来的组负责过多或过少的情况
				case joinOp:
					sc.lastResult[command.ClientId] = result{
						SerialNumber: command.SerialNumber,
					}
					// 开始newConfig的设置
					newConfig := Config{
						Num: sc.configs[len(sc.configs)-1].Num + 1,
						// Shards和Groups接下来设置
					}
					// 新的gid应该是输入方保证不与已有gid重复
					for k := range command.Servers {
						if _, ok := sc.configs[len(sc.configs)-1].Groups[k]; ok {
							panic("join指定的gid已经存在")
						}
					}
					// 设置Groups
					newConfig.Groups = make(map[int][]string, len(sc.configs[len(sc.configs)-1].Groups)+len(command.Servers))
					appendGroups(newConfig.Groups, sc.configs[len(sc.configs)-1].Groups)
					appendGroups(newConfig.Groups, command.Servers)
					// 设置Shards，就是从每个原来的组里分出多的shard给没满的，这样就能尽量避免不必要的shard传输
					count := make(map[int]int, len(newConfig.Groups))
					// 每个组的shard数量最大值的确是除了之后向上取整，但是注意测试里要求不能有两个组的shard数量相差大于1
					// 也就是如果10个shard，3个组，必须是4, 3, 3而不能是4, 4, 2。这个细节要注意处理
					nShardsPerGroup := int(math.Ceil(float64(NShards) / float64(len(newConfig.Groups))))
					for gid := range newConfig.Groups {
						count[gid] = 0
					}
					// 遍历原来的Shards，除了统计哪些组能接受shard分配以外，也将需要分配到其他组的shard的gid标为0，
					// 表示后续分配。这样的话，初始状态全0的情况就能在分配的时候不用单独写判断的代码
					newConfig.Shards = sc.configs[len(sc.configs)-1].Shards
					for i, gid := range sc.configs[len(sc.configs)-1].Shards {
						// 要把初始时的gid=0的情况排除
						if gid == 0 {
							continue
						}
						if count[gid]+1 > nShardsPerGroup {
							newConfig.Shards[i] = 0
						} else {
							count[gid]++
						}
					}
					sc.rebalance(&newConfig, count, nShardsPerGroup)
					// newConfig设置完成
					sc.configs = append(sc.configs, newConfig)

				case leaveOp:
					sc.lastResult[command.ClientId] = result{
						SerialNumber: command.SerialNumber,
					}
					// 开始newConfig的设置
					newConfig := Config{
						Num: sc.configs[len(sc.configs)-1].Num + 1,
						// Shards和Groups接下来设置
					}
					// 设置Groups
					newConfig.Groups = make(map[int][]string, len(sc.configs[len(sc.configs)-1].Groups))
					appendGroups(newConfig.Groups, sc.configs[len(sc.configs)-1].Groups)
					removeGroups(newConfig.Groups, command.GIDs)
					// 注意，有可能移除了所有的，此时要直接把所有的shard又分配给0，也就是表示没有组的情况
					if len(newConfig.Groups) == 0 {
						for i := range newConfig.Shards {
							newConfig.Shards[i] = 0
						}
					} else {
						// 设置Shards，思路就是将离开的组负责的shard分配给留下的组。
						// 如果留下的组本身负责的shard数量就超过此时计算的每个组应该负责的最大shard数量，
						// 也要做改变，一开始没有，然后发现测试对这个也是有要求的，数量平衡优先级高于传输数据少
						count := make(map[int]int, len(newConfig.Groups))
						for gid := range newConfig.Groups {
							count[gid] = 0
						}
						nShardsPerGroup := int(math.Ceil(float64(NShards) / float64(len(newConfig.Groups))))
						newConfig.Shards = sc.configs[len(sc.configs)-1].Shards
						sort.Ints(command.GIDs)
						for i, gid := range sc.configs[len(sc.configs)-1].Shards {
							// 这里可以不考虑gid为0，因为leave操作不应该是第一个进行的操作。我用panic作为检查
							if gid == 0 {
								panic("Leave操作发现上一个配置有gid为0")
							}
							// sort.SearchInts()的行为和sort.Search()不一样，前者返回的是查找值应当插入的位置，后者是返回第一个查找条件为true的位置
							// 当查找的值不存在时，前者未必返回的是len(data)，而后者一定是len(data)
							idx := sort.SearchInts(command.GIDs, gid)
							if idx < len(command.GIDs) && command.GIDs[idx] == gid {
								// 被移除的
								newConfig.Shards[i] = 0
							} else {
								// 没有移除的
								if count[gid]+1 > nShardsPerGroup {
									newConfig.Shards[i] = 0
								} else {
									count[gid]++
								}
							}
						}
						sc.rebalance(&newConfig, count, nShardsPerGroup)
					}
					// newConfig设置完成
					sc.configs = append(sc.configs, newConfig)

				case moveOp:
					sc.lastResult[command.ClientId] = result{
						SerialNumber: command.SerialNumber,
					}
					// 开始newConfig的设置
					newConfig := Config{
						Num: sc.configs[len(sc.configs)-1].Num + 1,
						// Shards和Groups接下来设置
					}
					// 设置Groups
					newConfig.Groups = make(map[int][]string, len(sc.configs[len(sc.configs)-1].Groups))
					appendGroups(newConfig.Groups, sc.configs[len(sc.configs)-1].Groups)
					// 设置Shards
					newConfig.Shards = sc.configs[len(sc.configs)-1].Shards
					newConfig.Shards[command.Shard] = command.GID
					// newConfig设置完成
					sc.configs = append(sc.configs, newConfig)

				case queryOp:
					index := len(sc.configs) - 1
					if command.Num >= 0 && command.Num <= index {
						index = command.Num
					}
					sc.lastResult[command.ClientId] = result{
						SerialNumber: command.SerialNumber,
						Config:       sc.configs[index],
					}

				default:
					// 会到这里说明有错
					panic("出现未定义的操作\n")
				}
			}
		} else {
			// 不应该会走到这里
			panic("msg.CommandValid是false\n")
		}
		sc.cond.Broadcast()
		sc.mu.Unlock()
	}
}

// join和leave给标记为0的shard分配gid的逻辑其实是一致的
func (sc *ShardCtrler) rebalance(newConfig *Config, count map[int]int, nShardsPerGroup int) {
	notFullGids := make([]int, 0, len(newConfig.Groups))
	keys := getSortedGids(newConfig.Groups)
	for _, k := range keys {
		if count[k] < nShardsPerGroup {
			notFullGids = append(notFullGids, k)
		}
	}
	// 开始给shard分配gid
	next := 0
	for i := range newConfig.Shards {
		if newConfig.Shards[i] == 0 {
			newConfig.Shards[i] = notFullGids[next]
			count[notFullGids[next]]++
			if count[notFullGids[next]] == nShardsPerGroup {
				next++
			} else if count[notFullGids[next]] > nShardsPerGroup {
				// 应该不会到这
				panic("新组负责的shard数量过多")
			}
		}
		// 不需要else，因为gid不变的shard在count里已有统计
	}
	// 最后要再做一些调整，将4, 4, 2这种调成4, 3, 3
	for {
		min := nShardsPerGroup + 1
		max := nShardsPerGroup - 3
		var minGid, maxGid int
		keys := getSortedGids(newConfig.Groups)
		for _, k := range keys {
			if count[k] > max {
				max = count[k]
				maxGid = k
			}
			if count[k] < min {
				min = count[k]
				minGid = k
			}
		}
		if max <= min+1 {
			break
		} else {
			count[maxGid]--
			count[minGid]++
			for i := range newConfig.Shards {
				if newConfig.Shards[i] == maxGid {
					newConfig.Shards[i] = minGid
					break
				}
			}
		}
	}
}

// 添加一些重复用到的功能的代码

// 将g2的内容添加到g1。两者的key要不重复
func appendGroups(g1, g2 map[int][]string) {
	for k, v := range g2 {
		if _, ok := g1[k]; ok {
			panic("两组groups的gid有重复")
		}
		g1[k] = make([]string, len(v))
		copy(g1[k], v)
	}
}

// 将指定的gid从g中移除。调用方保证gid都在g中存在
func removeGroups(g map[int][]string, gids []int) {
	for _, gid := range gids {
		if _, ok := g[gid]; !ok {
			panic("要离开的gid不存在")
		}
		delete(g, gid)
	}
}

// 排序是为了一些map的遍历能在不同服务器都以相同的顺序，从而保证shards的分配一致
func getSortedGids(groups map[int][]string) []int {
	gids := make([]int, 0, len(groups))
	for k := range groups {
		gids = append(gids, k)
	}
	sort.Ints(gids)
	return gids
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1) // lab4a这里是我自己仿照lab3加的
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.lastResult = make(map[int]result)
	sc.cond = *sync.NewCond(&sc.mu)

	go sc.applier()
	go sc.ticker()

	return sc
}
