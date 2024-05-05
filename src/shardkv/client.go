package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int
	serialNumber int
	// 没有用锁是因为lab3中就说了调用Clerk的get, put, append是串行的，一个结束才下一个
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end // make_end用来将Config里服务器字符串名映射为labrpc.ClientEnd
	// You'll have to add code here.
	ck.serialNumber = 0
	ck.clientId = int(nrand())
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	// fmt.Printf("client %d 进入get\n", ck.clientId)
	// defer fmt.Printf("client %d 退出get\n", ck.clientId)
	args := GetArgs{}
	args.Key = key

	args.ClientId = ck.clientId
	args.SerialNumber = ck.serialNumber
	ck.serialNumber++
	args.ConfigNum = ck.config.Num

	// 接下来就是向kv存储服务发送请求
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			// 这里和lab4a一样，没有像我的lab3的实现一样保存上次的leader先试一试，直接就是遍历
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				// 添加一个ErrConfigChanged，其处理和ErrWrongGroup其实一样，但逻辑上未必是不负责这个shard
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrConfigNotMatch) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		// 更新config后重试请求需要更新args里的ConfigNum
		args.ConfigNum = ck.config.Num
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// fmt.Printf("client %d 进入putappend\n", ck.clientId)
	// defer fmt.Printf("client %d 退出putappend\n", ck.clientId)
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	args.ClientId = ck.clientId
	args.SerialNumber = ck.serialNumber
	ck.serialNumber++
	args.ConfigNum = ck.config.Num

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				// fmt.Printf("reply.Err: %v\n", reply.Err)
				if ok && reply.Err == OK {
					return
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrConfigNotMatch) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		args.ConfigNum = ck.config.Num
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
