package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type Task int

const (
	Map Task = iota
	Reduce
	Wait
	Done
)

type RequestTaskArgs struct {
	WorkerID int
}
type RequestTaskReply struct {
	TaskID   int
	File     string
	TaskType Task
	NReduce  int
	NMap     int
}

type ReportTaskArgs struct {
	TaskID   int
	TaskType Task
}
type ReportTaskReply struct{}
