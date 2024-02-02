package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 是否打印调试信息
var debugEnabled = false

func debug(format string, a ...interface{}) {
	if debugEnabled {
		log.Printf(format, a...)
	}
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type Task struct {
	Type TaskType
	ID int
	Status TaskStatus
	FileName string
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	nMap int
	nReduce int
	mapTasks    []Task
	reduceTasks []Task
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	for i := range c.mapTasks {
		if c.mapTasks[i].Status == Idle {
			debug("coordinator: find new map task %v status %v\n", c.mapTasks[i].ID, c.mapTasks[i].Status)
			c.mapTasks[i].Status = InProgress
			c.mapTasks[i].StartTime = time.Now()	// 记录任务开始时间
			reply.Task = c.mapTasks[i]
			return nil
		}
	}

	for i := range c.mapTasks {
		if c.mapTasks[i].Status != Completed {
			reply.Task = Task{
				Type: Wait,
			}
			return nil
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == Idle {
			debug("coordinator: find new reduce task %v status %v\n", c.reduceTasks[i].ID, c.reduceTasks[i].Status)
			c.reduceTasks[i].Status = InProgress
			reply.Task = c.reduceTasks[i]
			return nil
		}
	}
	debug("coordinator: find no tasks remaining\n")
	reply.Task = Task{
		Type: Wait,		// 让worker等待coordinator退出
	}
	return nil

}

func (c *Coordinator) SetTaskStatus(args *SetTaskStatusArgs, reply *SetTaskStatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Task.Type == Map {
		c.mapTasks[args.Task.ID].Status = args.Status
		debug("coordinator: set map task %v to status %v\n", args.Task.ID, c.mapTasks[args.Task.ID].Status)
	} else if args.Task.Type == Reduce {
		c.reduceTasks[args.Task.ID].Status = args.Status
		debug("coordinator: set reduce task %v to status %v\n", args.Task.ID, c.reduceTasks[args.Task.ID].Status)
	} else {
		return errors.New("invalid task type")
	}
	return nil
}

// 检查并重置运行超时任务
func (c *Coordinator) checkTimeoutTasks() {
	for {
		time.Sleep(time.Second)	// 每秒检查一次

		c.mu.Lock()
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == InProgress && time.Since(c.mapTasks[i].StartTime) > 10*time.Second {
				debug("coordinator: map task %v timeout\n", c.mapTasks[i].ID)
				c.mapTasks[i].Status = Idle
			}
		}
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == InProgress && time.Since(c.reduceTasks[i].StartTime) > 10*time.Second {
				debug("coordinator: reduce task %v timeout\n", c.reduceTasks[i].ID)
				c.reduceTasks[i].Status = Idle
			}
		}
		c.mu.Unlock()
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, reduceTask := range c.reduceTasks {
		if reduceTask.Status != Completed {
			return false
		}
	}
	debug("coordinator exit!\n");
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		nMap: len(files),
		nReduce: nReduce,
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		mu: sync.Mutex{},
	}
	for i := range c.mapTasks {
		c.mapTasks[i].Type = Map
		c.mapTasks[i].ID = i
		c.mapTasks[i].Status = Idle
		c.mapTasks[i].FileName = files[i]
	}
	for i := range c.reduceTasks {
		c.reduceTasks[i].Type = Reduce
 		c.reduceTasks[i].ID = i
		c.reduceTasks[i].Status = Idle
	}
	debug("nMap = %v, nReduce = %v\n", c.nMap, c.nReduce)
	go c.checkTimeoutTasks()	// 启动检查超时任务的goroutine
	c.server()
	return &c
}
