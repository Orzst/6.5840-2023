package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
	// 再添加几个用于Err的字符串常量
	ErrWrongLeader        = "ErrWrongLeader"
	ErrOutdatedRequest    = "ErrOutdatedRequest"
	ErrCommitProbablyFail = "ErrCommitProbablyFail"
)

type Err string

// 要实现去重，这里的Args里就都和lab3一样的思路，加上客户ID和序号

type JoinArgs struct {
	Servers      map[int][]string // new GID -> servers mappings
	ClientId     int
	SerialNumber int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs         []int
	ClientId     int
	SerialNumber int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard        int
	GID          int
	ClientId     int
	SerialNumber int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num          int // desired config number
	ClientId     int
	SerialNumber int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
