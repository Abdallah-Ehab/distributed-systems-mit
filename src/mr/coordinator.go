package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status int

const (
	NotStarted Status = iota
	InProgress
	Completed
)

type TaskStatus struct {
	status    Status
	startTime time.Time
}

type Coordinator struct {
	mu sync.Mutex

	MapInputFiles  []string
	MapTasksStatus []TaskStatus
	CompletedMaps  int

	ReduceTasksStatus []TaskStatus
	CompletedReduces  int
	nReduce           int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := -1
	err := error(nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.CompletedMaps < len(c.MapInputFiles) {
		for i, t := range c.MapTasksStatus {
			if t.status == NotStarted {
				task = i
				break
			}
		}
		if task == -1 { // maps still in progress
			task, err = c.findStalledTask(Map)
			if err != nil {
				reply.TaskType = Wait
				return nil
			}
		}
		c.MapTasksStatus[task].status = InProgress
		c.MapTasksStatus[task].startTime = time.Now()
		reply.TaskID = task
		reply.File = c.MapInputFiles[task]
		reply.NReduce = c.nReduce
		reply.TaskType = Map
		// fmt.Printf("map task %d assigned to %d\n", task, args.WorkerID)
	} else if c.CompletedReduces < c.nReduce {
		for i, t := range c.ReduceTasksStatus {
			if t.status == NotStarted {
				task = i
				break
			}
		}
		if task == -1 { // reduces still in progress
			task, err = c.findStalledTask(Reduce)
			if err != nil {
				reply.TaskType = Wait
				return nil
			}
		}
		c.ReduceTasksStatus[task].status = InProgress
		c.ReduceTasksStatus[task].startTime = time.Now()
		reply.TaskID = task
		reply.NMap = len(c.MapInputFiles)
		reply.TaskType = Reduce
		// fmt.Printf("reduce task %d assigned to %d\n", reply.TaskID, args.WorkerID)
	} else {
		reply.TaskType = Done
	}
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == Map {
		c.MapTasksStatus[args.TaskID].status = Completed
		c.CompletedMaps += 1
	} else if args.TaskType == Reduce {
		c.ReduceTasksStatus[args.TaskID].status = Completed
		c.CompletedReduces += 1
	}

	// fmt.Printf("task %d finished\n", args.TaskID)
	return nil
}

func (c *Coordinator) findStalledTask(taskType Task) (int, error) {
	var tasks []TaskStatus
	if taskType == Map {
		tasks = c.MapTasksStatus
	} else if taskType == Reduce {
		tasks = c.ReduceTasksStatus
	}

	for i, t := range tasks {
		if t.status == InProgress && time.Since(t.startTime) > 10*time.Second {
			return i, nil
		}
	}
	return -1, fmt.Errorf("no stuck tasks")
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CompletedReduces == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapInputFiles = files
	c.nReduce = nReduce
	c.MapTasksStatus = make([]TaskStatus, len(files))
	c.ReduceTasksStatus = make([]TaskStatus, nReduce)
	c.CompletedMaps = 0
	c.CompletedReduces = 0
	c.server(sockname)
	return &c
}
