package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	// 自己添加需要的Err
	ErrOutdatedRequest    = "ErrOutdatedRequest"
	ErrCommitProbablyFail = "ErrCommitProbablyFail"
	ErrConfigNotMatch     = "ErrConfigNotMatch"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId     int
	SerialNumber int
	// 用来检查Config是否与客户发送请求时的一致，
	// 这可能比直接检查是否还负责请求的shard更严格，导致不必要的重发
	// 但实现起来会更容易，先保证正确运行再考虑改进
	ConfigNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId     int
	SerialNumber int
	ConfigNum    int
}

type GetReply struct {
	Err   Err
	Value string
}
