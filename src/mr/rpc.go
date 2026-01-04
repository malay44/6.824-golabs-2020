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

type TaskStatus int
type TaskCategory int

// task status
const (
	NOT_STARTED TaskStatus = iota
	IN_PROGRESS
	COMPLETED
)

// task category
const (
	MAP TaskCategory = iota
	REDUCE
	WAIT
)

type Task struct {
	TaskInfo
	mu sync.Mutex
}

type TaskInfo struct {
	Filename  string
	Status    TaskStatus
	Category  TaskCategory
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
