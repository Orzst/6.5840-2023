package kvraft

import (
	"crypto/rand"
	"math/big"

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
	// DPrintf("%v\n%d的clerk.Get(%s)调用开始\n", time.Now(), ck.clientId, key)
	// defer DPrintf("%v\n%d的clerk.Get(%s)调用结束\n", time.Now(), ck.clientId, key)

	args := GetArgs{
		Key:          key,
		ClientId:     ck.clientId,
		SerialNumber: ck.serialNumber,
	}
	ck.serialNumber++
	reply := GetReply{}
	// lab3这里失败的请求允许一直重试等待，直到成功
	for {
		svcMeth := "KVServer.Get"
		// Call不保证一定能返回true，因为有时就是想测某个服务器出问题的情况
		// 所以逻辑应该是如果有网络问题，也和不确定leader一样，轮流找一遍
		// 之前假定不断重试一定能true，就死循环了，没有正确理解题意
		// 但其实可以允许一定次数的重试，以便网络波动的时候不至于误以为需要遍历所有
		ok := ck.callUntilSuccessWithLimit(ck.leaderId, 10, svcMeth, &args, &reply)
		// 上面这个更改后，能继续过几个测试，但是到了有network partition的测试又失败了
		// 想了一下，如果发生了network partition，而我当前的leader到了服务器数量少的分区，
		// 则回应是commitFail，但只要network partition继续存在，它仍然会认为自己是leader
		// 导致在我的逻辑中，一致是在尝试给它重发，所以其实除了Err = OK的情况，都应该进行
		// 全部遍历的尝试
		if ok && reply.Err == OK {
			// 能收到回复，服务器确实为leader且得到了结果
			// DPrintf("%v\n%d的clerk.Get(%s)调用结束\n得到的值是%s\n", time.Now(), ck.clientId, key, reply.Value)
			return reply.Value
		} else {
			// 其他各种情况，包括收不到回复，服务器不为leader，虽然认为自己一直是leader但实际有network partition

			// 注意遍历过程中leader可能改变为已遍历过的当时不是leader的服务器
			// 但最外层循环保证了这种情况也会进行重试
			for i := range ck.servers {
				// ck.callUntilSuccess(i, svcMeth, &args, &reply)
				ok = ck.callUntilSuccessWithLimit(i, 10, svcMeth, &args, &reply)
				if ok && reply.Err == OK {
					// 服务器i为leader
					ck.leaderId = i
					// DPrintf("%v\n%d的clerk.Get(%s)调用结束\n得到的值是%s\n", time.Now(), ck.clientId, key, reply.Value)
					return reply.Value
				}
			}
		}
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
	// DPrintf("%v\n%d的clerk.PutAppend(%s, %s, %s)调用开始\n", time.Now(), ck.clientId, key, value, op)
	// defer DPrintf("%v\n%d的clerk.PutAppend(%s, %s, %s)调用结束\n", time.Now(), ck.clientId, key, value, op)
	args := PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		ClientId:     ck.clientId,
		SerialNumber: ck.serialNumber,
	}
	ck.serialNumber++
	reply := PutAppendReply{}
	for {
		svcMeth := "KVServer.PutAppend"
		ok := ck.callUntilSuccessWithLimit(ck.leaderId, 10, svcMeth, &args, &reply)
		if ok && reply.Err == OK {
			return
		} else {
			for i := range ck.servers {
				ok = ck.callUntilSuccessWithLimit(i, 10, svcMeth, &args, &reply)
				if ok && reply.Err == OK {
					ck.leaderId = i
					return
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

// // 自己添加一个获取clientId的rpc调用，顺便也能获取一开始的leaderId。暂时不实现这个方案了
//
//	func (ck *Clerk) getClientIdAndLeaderId() (clientId, leaderId int) {
//		args := GetClientIdAndLeaderIdArgs{}
//		reply := GetClientIdAndLeaderIdReply{}
//		for {
//			for i := range ck.servers {
//				ok := ck.servers[i].Call("KVServer.GetClientAndLeaderId", &args, &reply)
//				if ok && reply.LeaderId != -1 {
//					return reply.ClientId, reply.LeaderId
//				}
//			}
//		}
//	}
func (ck *Clerk) callUntilSuccessWithLimit(server, limit int, svcMeth string, args, reply interface{}) (ok bool) {
	for i := 0; i < limit; i++ {
		ok = ck.servers[server].Call(svcMeth, args, reply)
		if ok {
			break
		}
	}
	return ok
}
