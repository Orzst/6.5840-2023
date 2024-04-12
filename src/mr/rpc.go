package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	TaskGot bool
	File    string
	TaskID  int
	NReduce int
}

type DoneMapTaskArgs struct {
	TaskID int
}
type DoneMapTaskReply struct {
}

type GetReduceTaskArgs struct {
}

type GetReduceTaskReply struct {
	MapAllDone bool
	TaskGot    bool
	TaskID     int
	NMap       int
}
type DoneReduceTaskArgs struct {
	TaskID int
}
type DoneReduceTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
