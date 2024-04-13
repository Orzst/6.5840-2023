package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 为了排序，从mrsequential.go里抄的
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 获取map task
	// 具体来说，是获取一个map的输入文件名，并了解是否还有map task
	// 如果有map task，就根据输入文件名来处理
	// 注意，实验中map函数要的是文件名和文件内容，
	// 所以worker要自己根据文件名读取内容
	for mapAllDone, file, taskID, nReduce := CallGetMapTask(); !mapAllDone; mapAllDone, file, taskID, nReduce = CallGetMapTask() {
		// 懒得写得更正常了。总之就是有个问题是可能taskGot == false，worker开始要reduce task了，
		// 但这时coordinator发现map task有出错的，但是没有worker要map task了
		// 这个问题和我把两个task分开写也有关系
		// 所以我这里改成判断是否mapAllDone
		// 没有都结束，但是taskID为-1表示有任务正在进行，但不知道会不会出问题
		if taskID == -1 {
			d, _ := time.ParseDuration("1s")
			time.Sleep(d)
			continue
		}

		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		content, err := io.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		f.Close()
		kva := mapf(file, string(content))
		sort.Sort(ByKey(kva))
		// 接下来将排序好的结果，按照ihash的结果，存到各个intermedia file里
		parts := make([][]KeyValue, nReduce)
		for _, kv := range kva {
			index := ihash(kv.Key) % nReduce
			parts[index] = append(parts[index], kv)
		}
		for i, part := range parts {
			intermediateFile := fmt.Sprintf("mr-%d-%d", taskID, i)
			f, err := os.CreateTemp(".", "tmpfile-*")
			if err != nil {
				log.Fatalf("cannot create temporary file %v", f.Name())
			}
			jsonPart, _ := json.Marshal(part)
			io.WriteString(f, string(jsonPart))
			f.Close()
			os.Rename(f.Name(), intermediateFile) // 这里error懒得考虑了
		}

		CallDoneMapTask(taskID)
	}
	// 如果map task没了，就开始获取reduce task
	// 具体来说，先要判断map阶段是否完成。然后再和map阶段一样判断是否还有reduce task
	// 如果有reduce task，则每个文件用reduce函数处理
	// 如果没有，就结束了
	for reduceAllDone, taskID, nMap := CallGetReduceTask(); !reduceAllDone; reduceAllDone, taskID, nMap = CallGetReduceTask() {
		if taskID == -1 {
			d, _ := time.ParseDuration("1s")
			time.Sleep(d)
			continue
		}

		reduceInput := map[string][]string{}
		for i := 0; i < nMap; i++ {
			file := fmt.Sprintf("mr-%d-%d", i, taskID)
			f, err := os.Open(file)
			if err != nil {
				log.Fatalf("cannot open intermediate file: %s", file)
			}
			jsonData, _ := io.ReadAll(f)
			f.Close()
			data := []KeyValue{}
			json.Unmarshal(jsonData, &data)
			for _, kv := range data {
				reduceInput[kv.Key] = append(reduceInput[kv.Key], kv.Value)
			}
		}
		keys := make([]string, 0, len(reduceInput))
		for k := range reduceInput {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		outputFile := fmt.Sprintf("mr-out-%d", taskID)
		f, err := os.CreateTemp(".", "tmpfile-*")
		if err != nil {
			log.Fatalf("cannot create temporary file: %v", f.Name())
		}
		for _, k := range keys {
			output := reducef(k, reduceInput[k])
			fmt.Fprintf(f, "%v %v\n", k, output)
		}
		f.Close()
		os.Rename(f.Name(), outputFile)

		CallDoneReduceTask(taskID)
	}
}

func CallGetMapTask() (mapAllDone bool, file string, taskID int, nReduce int) {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		fmt.Printf("reply.mapAllDone: %t\n", reply.MapAllDone)
		if !reply.MapAllDone {
			fmt.Printf("reply.TaskID: %d\n", reply.TaskID)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.MapAllDone, reply.File, reply.TaskID, reply.NReduce
}

func CallDoneMapTask(taskID int) {
	args := DoneMapTaskArgs{TaskID: taskID}
	reply := DoneMapTaskReply{}
	ok := call("Coordinator.DoneMapTask", &args, &reply)
	if ok {
		fmt.Printf("send message succeeded: map task %d done.\n", taskID)
	} else {
		fmt.Printf("send message failed: map task %d done.\n", taskID)
	}
}

// 不需要返回文件名，因为约定了intermediate file叫mr-X-Y
func CallGetReduceTask() (mapAllDone bool, taskID int, nMap int) {
	args := GetReduceTaskArgs{}
	reply := GetReduceTaskReply{}
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		fmt.Printf("reply.ReduceAllDone: %t\n", reply.ReduceAllDone)
		if !reply.ReduceAllDone {
			fmt.Printf("reply.TaskID: %d\n", reply.TaskID)
		}
	} else {
		fmt.Printf("call failed\n")
	}
	return reply.ReduceAllDone, reply.TaskID, reply.NMap
}

func CallDoneReduceTask(taskID int) {
	args := DoneReduceTaskArgs{TaskID: taskID}
	reply := DoneReduceTaskReply{}
	ok := call("Coordinator.DoneReduceTask", &args, &reply)
	if ok {
		fmt.Printf("send message succeeded: reduce task %d done.\n", taskID)
	} else {
		fmt.Printf("send message failed: reduce task %d done.\n", taskID)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
