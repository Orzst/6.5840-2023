package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	// 自己再添加几个常量
	ErrOutdatedRequest = "ErrOutdatedRequest"
	ErrCommitFail      = "ErrCommitFail"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId, SerialNumber int
}

// 各个Err用到的值是上面那几个常量

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId, SerialNumber int
}

type GetReply struct {
	Err   Err
	Value string
}

// // 自己添加一个clerk初始化时获取clientId和当时的leaderId的rpc。暂时不实现这个方案了
// type GetClientIdAndLeaderIdArgs struct {
// }

// type GetClientIdAndLeaderIdReply struct {
// 	ClientId int
// 	LeaderId int // -1表示当前服务器非Leader，此时ClientId没有意义
// }
