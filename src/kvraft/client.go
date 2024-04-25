package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId     int
	serialNumber int
	leaderId     int
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
	// You'll have to add code here.
	ck.serialNumber = 0
	ck.clientId = int(nrand()) // 直接随机数生成吧，lab好像是允许这么做的
	// ck.clientId, ck.leaderId = ck.getClientIdAndLeaderId()
	ck.leaderId = 0 // 就直接假定一开始的leader是0，这样初始情况也可以包含在发送的服务器不再是leader的情况处理里
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// 如果key不存在，返回空字符串""
// keeps trying forever in the face of all other errors.
// 其他异常就一直重试
//
// you can send an RPC with code like this:
// 调用labrpc里的rpc的格式
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("%v\n%d的clerk.Get(%s)调用开始\n", time.Now(), ck.clientId, key)
	defer DPrintf("%v\n%d的clerk.Get(%s)调用结束\n", time.Now(), ck.clientId, key)

	args := GetArgs{
		Key:          key,
		ClientId:     ck.clientId,
		SerialNumber: ck.serialNumber,
	}
	ck.serialNumber++
	reply := GetReply{Err: "test_Get"}
	// lab3这里失败的请求允许一直重试等待，直到成功
	for {
		svcMeth := "KVServer.Get"
		ck.callUntilSuccess(ck.leaderId, svcMeth, &args, &reply)

		if reply.Err == OK {
			// 服务器确实为leader且得到了结果
			// DPrintf("%v\n%d的clerk.Get(%s)调用结束\n得到的值是%s\n", time.Now(), ck.clientId, key, reply.Value)
			return reply.Value
		} else if reply.Err == ErrWrongLeader {
			// 请求的服务器不再是leader

			// 注意遍历过程中leader可能改变为已遍历过的当时不是leader的服务器
			// 但最外层循环保证了这种情况也会进行重试
			for i := range ck.servers {
				ck.callUntilSuccess(i, svcMeth, &args, &reply)

				if reply.Err != ErrWrongLeader {
					// 服务器i为leader
					ck.leaderId = i
					// 但未必是OK，也有可能这次操作未能在raft中commit
					if reply.Err == OK {
						// DPrintf("%v\n%d的clerk.Get(%s)调用结束\n得到的值是%s\n", time.Now(), ck.clientId, key, reply.Value)
						return reply.Value
					}
					break
				}
			}
		}
		// 其他情况说明这一次请求的操作没能成功，就重发
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// 这部分代码整体结构和Get里的一致
	DPrintf("%v\n%d的clerk.PutAppend(%s, %s, %s)调用开始\n", time.Now(), ck.clientId, key, value, op)
	defer DPrintf("%v\n%d的clerk.PutAppend(%s, %s, %s)调用结束\n", time.Now(), ck.clientId, key, value, op)
	args := PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		ClientId:     ck.clientId,
		SerialNumber: ck.serialNumber,
	}
	ck.serialNumber++
	reply := PutAppendReply{Err: "test_PutAppend"}
	for {
		svcMeth := "KVServer.PutAppend"
		ck.callUntilSuccess(ck.leaderId, svcMeth, &args, &reply)

		if reply.Err == OK {
			return
		} else if reply.Err == ErrWrongLeader {
			for i := range ck.servers {
				ck.callUntilSuccess(i, svcMeth, &args, &reply)

				if reply.Err != ErrWrongLeader {
					ck.leaderId = i
					if reply.Err == OK {
						return
					}
					break
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) callUntilSuccess(server int, svcMeth string, args interface{}, reply interface{}) {
	DPrintf("%v\nclient %d 进入callUntilSuccess\n", time.Now(), ck.clientId)
	defer DPrintf("%v\nclient %d 离开callUntilSuccess\n", time.Now(), ck.clientId)
	ok := ck.servers[server].Call(svcMeth, args, reply)
	for !ok {
		time.Sleep(1000 * time.Millisecond)
		ok = ck.servers[server].Call(svcMeth, args, reply)
		DPrintf("reply: %v\n", reply)
	}
}

// // 自己添加一个获取clientId的rpc调用，顺便也能获取一开始的leaderId。暂时不实现这个方案了
// func (ck *Clerk) getClientIdAndLeaderId() (clientId, leaderId int) {
// 	args := GetClientIdAndLeaderIdArgs{}
// 	reply := GetClientIdAndLeaderIdReply{}
// 	for {
// 		for i := range ck.servers {
// 			ok := ck.servers[i].Call("KVServer.GetClientAndLeaderId", &args, &reply)
// 			if ok && reply.LeaderId != -1 {
// 				return reply.ClientId, reply.LeaderId
// 			}
// 		}
// 	}
// }
