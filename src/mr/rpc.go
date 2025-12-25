package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
	"time"
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

type EmptyArgs struct{}

// task status
const NOT_STARTED = "not_started"
const IN_PROGRESS = "in_progress"
const COMPLETED = "completed"
const WAIT = "wait"

// task category
const MAP = "map"
const REDUCE = "reduce"

type Task struct {
	TaskInfo
	mu sync.Mutex
}

type TaskInfo struct {
	Filename  string
	Status    string
	Category  string
	StartTime time.Time
	TaskNo    int
}

type GetTaskReply struct {
	TaskInfo TaskInfo
	NReduce  int
}

type MarkTaskDoneArgs struct {
	TaskInfo TaskInfo
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
