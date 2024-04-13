package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	nMap, nReduce int
	inputFiles    []string
	// 改成计时，这样的话状态也只需要完成和未完成两种，bool就行
	mapTasksFinished, reduceTasksFinished []bool
	mapTasksIssued, reduceTasksIssued     []time.Time
}

// Your code here -- RPC handlers for the worker to call.

// 获得map task
func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 先简单点，就直接遍历找
	// worker提前退出的情况，就通过判断是否超过10s
	taskID := -1
	for i, finished := range c.mapTasksFinished {
		if !finished && (c.mapTasksIssued[i].IsZero() || time.Since(c.mapTasksIssued[i]).Seconds() > 10) {
			taskID = i
			break
		}
	}

	if taskID != -1 {
		reply.TaskGot = true
		reply.File = c.inputFiles[taskID]
		reply.TaskID = taskID
		reply.NReduce = c.nReduce
		c.mapTasksIssued[taskID] = time.Now()
	} else {
		reply.TaskGot = false
	}
	return nil
}

func (c *Coordinator) DoneMapTask(args *DoneMapTaskArgs, reply *DoneMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTasksFinished[args.TaskID] = true
	return nil
}

// 获得reduce task
func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 先简单点，遍历看是否map都完成了
	mapAllDone := true
	for _, finished := range c.mapTasksFinished {
		if !finished {
			mapAllDone = false
			break
		}
	}
	reply.MapAllDone = mapAllDone
	if !mapAllDone {
		return nil
	}

	// worker提前退出的情况，就通过判断是否超过10s
	taskID := -1
	for i, finished := range c.reduceTasksFinished {
		if !finished && (c.reduceTasksIssued[i].IsZero() || time.Since(c.reduceTasksIssued[i]).Seconds() > 10) {
			taskID = i
			break
		}
	}

	if taskID != -1 {
		reply.TaskGot = true
		reply.TaskID = taskID
		reply.NMap = c.nMap
		c.reduceTasksIssued[taskID] = time.Now()
	} else {
		reply.TaskGot = false
	}
	return nil
}
func (c *Coordinator) DoneReduceTask(args *DoneReduceTaskArgs, reply *DoneReduceTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTasksFinished[args.TaskID] = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, finished := range c.reduceTasksFinished {
		if !finished {
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
	c.mapTasksFinished = make([]bool, c.nMap)
	c.mapTasksIssued = make([]time.Time, c.nMap)
	c.reduceTasksFinished = make([]bool, c.nReduce)
	c.reduceTasksIssued = make([]time.Time, c.nReduce)

	c.server()
	return &c
}
