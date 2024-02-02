package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		assignTaskReply := callAssignTask()
		switch assignTaskReply.Task.Type {
		case Wait:
			time.Sleep(time.Millisecond * 100)
		case Exit:
			debug("worker: no task assigned, worker exit!\n");
			return
		case Map:
			debug("worker: map task %v assigned\n", assignTaskReply.Task.ID);
			doMapTask(assignTaskReply, mapf)
		case Reduce:
			debug("worker: reduce task %v assigned\n", assignTaskReply.Task.ID);
			doReduceTask(assignTaskReply, reducef)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(assignTaskReply AssignTaskReply, mapf func(string, string) []KeyValue) {
	mapTask := assignTaskReply.Task
	nReduce := assignTaskReply.NReduce

	// 读取输入文件，进行map
	fileName := mapTask.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	intermediate := mapf(fileName, string(content))

	// 将intermediate分组存入group，group第0维下标是reduce任务编号
	group := make([][]KeyValue, nReduce)

	for _, kv := range intermediate {
		// 计算该key的reduce任务编号
		reduceID := ihash(kv.Key) % nReduce
		group[reduceID] = append(group[reduceID], kv)
	}

	for i := 0; i < nReduce; i++ {
		// 创建中间文件
		imFileName := fmt.Sprintf("mr-%d-%d", mapTask.ID, i)
		imFile, err := ioutil.TempFile("", imFileName)
		if err != nil {
			log.Fatalf("cannot create im file: %v", err)
		}
		
		// 遍历中间键值对，编码为json，写入对应文件
		enc := json.NewEncoder(imFile)
		for _, kv := range group[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode key-value pair: %v", err)
			}
		}

		imFile.Close()
		os.Rename(imFile.Name(), imFileName)
	}

	// 标记任务已完成
	callSetTaskStatus(mapTask, Completed)
}

func doReduceTask(assignTaskReply AssignTaskReply, reducef func(string, []string) string) {
	reduceTask := assignTaskReply.Task
	nMap := assignTaskReply.NMap
	
	// 遍历reduce任务对应的中间文件，解码json到intermediate
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		imFileName := fmt.Sprintf("mr-%d-%d", i, reduceTask.ID)
		imFile, err := os.Open(imFileName)
		if err != nil {
			log.Fatalf("cannot open %v", imFileName)
		}
		dec := json.NewDecoder(imFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		imFile.Close()
	}

	// 创建out文件
	oFileName := fmt.Sprintf("mr-out-%d", reduceTask.ID)
	oFile, err := ioutil.TempFile("", oFileName)
	if err != nil {
		log.Fatalf("cannot create out file: %v", err)
	}

	// 排序，按相同键分组，进行reduce，并将结果写入out文件
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	oFile.Close()
	os.Rename(oFile.Name(), oFileName)

	// 标记任务已完成
	callSetTaskStatus(reduceTask, Completed)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func callAssignTask() AssignTaskReply {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		// 调用失败，说明coordinator已退出，此时worker应退出
		reply.Task.Type = Exit
		debug("worker: call Coordinator.AssignTask failed, worker will exit\n")
	}
	return reply
}

func callSetTaskStatus(task Task, taskStatus TaskStatus) {
	args := SetTaskStatusArgs{
		Task: task,
		Status: taskStatus,
	}
	reply := SetTaskStatusReply{};
	ok := call("Coordinator.SetTaskStatus", &args, &reply)
	debug("worker: ask to set task %v to %v\n", task.ID, taskStatus)
	if !ok {
		debug("worker: call Coordinator.SetTaskStatus failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// 这里做了更改，原本是报错并直接退出，现在返回false，worker退出的逻辑放到外层的callAssignTask中
		debug("worker: dialing: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
