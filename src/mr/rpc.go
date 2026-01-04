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

// RPC definitions here.

type EmptyArgs struct{}

// task status
const NOT_STARTED = "not_started"
const IN_PROGRESS = "in_progress"
const COMPLETED = "completed"

// task category
const MAP = "map"
const REDUCE = "reduce"
const WAIT = "wait"

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
	NMap     int // Number of map tasks (needed for reduce tasks to locate all input files)
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
