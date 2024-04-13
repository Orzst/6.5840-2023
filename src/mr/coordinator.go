package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskState int

const (
	Unassigned TaskState = iota
	Assigned
	Done
)

type Coordinator struct {
	// Your definitions here.
	nMap, nReduce                   int
	inputFiles                      []string
	mapTaskStates, reduceTaskStates []TaskState
	// 每个任务的状态（未分配，已分配未完成，已完成）
	// 出错时状态始终是已分配未完成
}

// Your code here -- RPC handlers for the worker to call.

// 获得map task
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	// 先简单点，就直接遍历找有没有未分配的任务
	taskID := -1
	for i, state := range c.mapTaskStates {
		if state == Unassigned {
			taskID = i
			break
		}
	}

	// 再考虑一下worker提前退出的情况，也就是任务状态为Assigned之后未必会Done
	if taskID == -1 {
		d, _ := time.ParseDuration("1s")
		time.Sleep(d)
		for i, state := range c.mapTaskStates {
			if state == Assigned {
				taskID = i
				break
			}
		}
	}

	if taskID != -1 {
		reply.TaskGot = true
		reply.File = c.inputFiles[taskID]
		reply.TaskID = taskID
		reply.NReduce = c.nReduce
		c.mapTaskStates[taskID] = Assigned
	} else {
		reply.TaskGot = false
	}
	return nil
}

func (c *Coordinator) DoneMapTask(args *DoneMapTaskArgs, reply *DoneMapTaskReply) error {
	c.mapTaskStates[args.TaskID] = Done
	return nil
}

// 获得reduce task
func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	// 先简单点，遍历看是否map都完成了
	mapAllDone := true
	for _, state := range c.mapTaskStates {
		if state != Done {
			mapAllDone = false
			break
		}
	}
	reply.MapAllDone = mapAllDone
	if !mapAllDone {
		return nil
	}

	taskID := -1
	for i, state := range c.reduceTaskStates {
		if state == Unassigned {
			taskID = i
			break
		}
	}

	// 再考虑一下worker提前退出的情况，也就是任务状态为Assigned之后未必会Done
	if taskID == -1 {
		d, _ := time.ParseDuration("1s")
		time.Sleep(d)
		for i, state := range c.reduceTaskStates {
			if state == Assigned {
				taskID = i
				break
			}
		}
	}

	if taskID != -1 {
		reply.TaskGot = true
		reply.TaskID = taskID
		reply.NMap = c.nMap
		c.reduceTaskStates[taskID] = Assigned
	} else {
		reply.TaskGot = false
	}
	return nil
}
func (c *Coordinator) DoneReduceTask(args *DoneReduceTaskArgs, reply *DoneReduceTaskReply) error {
	c.reduceTaskStates[args.TaskID] = Done
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
	ret = true
	for _, state := range c.reduceTaskStates {
		if state != Done {
			ret = false
			break
		}
	}

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
	c.mapTaskStates = make([]TaskState, c.nMap)
	for i := range c.mapTaskStates {
		c.mapTaskStates[i] = Unassigned
	}
	c.reduceTaskStates = make([]TaskState, c.nReduce)
	for i := range c.reduceTaskStates {
		c.reduceTaskStates[i] = Unassigned
	}

	c.server()
	return &c
}
