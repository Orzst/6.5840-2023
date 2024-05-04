package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

// 和lab3相似的地方可以直接用过来改改
// lab3出现过的注释都删掉了
// 几个RPC的实现就尽量不动它已有的少量代码
// 它直接就是网络问题!ok也遍历，我lab3里是考虑先重试几次
// 而且也不考虑保存上一次的leaderID，代码上会简洁点

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId     int
	serialNumber int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.serialNumber = 0
	ck.clientId = int(nrand())
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	args.ClientId = ck.clientId
	args.SerialNumber = ck.serialNumber
	ck.serialNumber++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			// 注意几个RPC里都添加了reply.Err == OK
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	args.ClientId = ck.clientId
	args.SerialNumber = ck.serialNumber
	ck.serialNumber++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	args.ClientId = ck.clientId
	args.SerialNumber = ck.serialNumber
	ck.serialNumber++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.ClientId = ck.clientId
	args.SerialNumber = ck.serialNumber
	ck.serialNumber++

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
