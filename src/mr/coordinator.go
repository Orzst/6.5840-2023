package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce     int
	inputFiles        []string
	curMap, curReduce int
	// 暂时没有考虑task出错的问题
	// 如果要考虑，可以改用一个int slice记录每个任务的状态（未分配，已分配未完成，已完成）
	// 而非curXXX，每次给的是未分配的
	// 出错时通过rpc将状态改回未分配
}

// Your code here -- RPC handlers for the worker to call.

// 获得map task
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.curMap++
	if c.curMap < c.nMap {
		reply.TaskGot = true
		reply.File = c.inputFiles[c.curMap]
		reply.TaskID = c.curMap
		reply.NReduce = c.nReduce
	} else {
		reply.TaskGot = false
	}
	return nil
}

// 获得reduce task
func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.curReduce++
	if c.curReduce < c.nReduce {
		reply.TaskGot = true
		reply.TaskID = c.curReduce
		reply.NMap = c.nMap
	} else {
		reply.TaskGot = false
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// 文件数量就是map task数量，nReduce则是reduce task数量
	c.nMap = len(files)
	c.nReduce = nReduce
	c.inputFiles = append(c.inputFiles, files...)
	c.curMap = -1
	c.curReduce = -1

	c.server()
	return &c
}
