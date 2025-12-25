package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Master struct {
	// Your definitions here.
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *EmptyArgs, reply *GetTaskReply) error {
	Debug.Printf("got task request")
	task := m.findTask()
	reply.TaskInfo = task.TaskInfo
	reply.NReduce = m.nReduce
	Debug.Printf("sent task, %v", task.TaskInfo.Filename)
	return nil
}

func (m *Master) MarkTaskDone(args *MarkTaskDoneArgs, reply *EmptyArgs) error {
	switch args.TaskInfo.Category {
	case MAP:
		for i := range m.mapTasks {
			task := &m.mapTasks[i]
			task.mu.Lock()
			if task.Filename == args.TaskInfo.Filename {
				task.Status = COMPLETED
			}
			task.mu.Unlock()
		}
	case REDUCE:
		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			task.mu.Lock()
			if task.Filename == args.TaskInfo.Filename {
				task.Status = COMPLETED
			}
			task.mu.Unlock()
		}
	}
	Debug.Printf("marked task as DONE, %v", args.TaskInfo.Filename)
	return nil
}

func (m *Master) findTask() *Task {
	completedMapTasks := 0
	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		Debug.Printf("waiting for lock of map task %v", i)
		task.mu.Lock()
		Debug.Printf("got lock of map task %v status: %v", i, task.Status)
		switch task.Status {
		case NOT_STARTED:
			task.Status = IN_PROGRESS
			task.Category = MAP
			task.StartTime = time.Now()
			task.mu.Unlock()
			return task
		case COMPLETED:
			completedMapTasks++
		default:
			task.mu.Unlock()
			continue
		}
		task.mu.Unlock()
	}

	if len(m.mapTasks) == completedMapTasks {
		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			task.mu.Lock()
			if task.Status == NOT_STARTED {
				task.Status = IN_PROGRESS
				task.Category = REDUCE
				task.StartTime = time.Now()
				task.mu.Unlock()
				return task
			}
			task.mu.Unlock()
		}
	}
	return &Task{TaskInfo: TaskInfo{Category: WAIT}}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := []Task{}
	for i, file := range files {
		Debug.Printf("Added %v map task, filename: %v", i, file)
		mapTasks = append(
			mapTasks,
			Task{
				TaskInfo: TaskInfo{
					Filename: file,
					Status:   NOT_STARTED,
					TaskNo:   i,
				},
			},
		)
	}

	reduceTasks := []Task{}
	for i := range nReduce {
		reduceTasks = append(
			reduceTasks,
			Task{
				TaskInfo: TaskInfo{
					Filename: "r-in-" + strconv.Itoa(i),
					Status:   NOT_STARTED,
					TaskNo:   i,
				},
			},
		)
	}

	m := Master{
		nReduce:     nReduce,
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
	}

	m.server()
	return &m
}
