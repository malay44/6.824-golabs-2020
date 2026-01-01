package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	TaskTimeout = 10 * time.Second // Timeout for task completion
)

type Master struct {
	// Your definitions here.
	nReduce     int
	nMap        int // Number of map tasks
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
	reply.NMap = m.nMap
	Debug.Printf("sent task, filename: %v, category: %v, taskNo: %v", task.Filename, task.Category, task.TaskNo)
	return nil
}

func (m *Master) MarkTaskDone(args *MarkTaskDoneArgs, reply *EmptyArgs) error {
	switch args.TaskInfo.Category {
	case MAP:
		for i := range m.mapTasks {
			task := &m.mapTasks[i]
			task.mu.Lock()
			if task.TaskNo == args.TaskInfo.TaskNo && task.Filename == args.TaskInfo.Filename {
				task.Status = COMPLETED
			}
			task.mu.Unlock()
		}
	case REDUCE:
		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			task.mu.Lock()
			if task.TaskNo == args.TaskInfo.TaskNo {
				task.Status = COMPLETED
			}
			task.mu.Unlock()
		}
	}
	Debug.Printf("marked task as DONE, category: %v, taskNo: %v", args.TaskInfo.Category, args.TaskInfo.TaskNo)
	return nil
}

func (m *Master) findTask() *Task {
	completedMapTasks := 0
	completedReduceTasks := 0
	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		task.mu.Lock()
		switch task.Status {
		case NOT_STARTED:
			task.Status = IN_PROGRESS
			task.Category = MAP
			task.StartTime = time.Now()
			task.mu.Unlock()
			Debug.Printf("found not started map task %v", i)
			return task
		case IN_PROGRESS:
			// Check if task has timed out (more than 10 seconds)
			if time.Since(task.StartTime) > TaskTimeout {
				Debug.Printf("map task %v timed out, reassigning", i)
				task.Status = IN_PROGRESS // Reassign to new worker
				task.StartTime = time.Now()
				task.mu.Unlock()
				return task
			}
			// Task is still in progress and hasn't timed out
			task.mu.Unlock()
			continue
		case COMPLETED:
			completedMapTasks++
			task.mu.Unlock()
		default:
			task.mu.Unlock()
			continue
		}
	}

	if len(m.mapTasks) == completedMapTasks {
		completedReduceTasks = 0
		for i := range m.reduceTasks {
			task := &m.reduceTasks[i]
			task.mu.Lock()
			switch task.Status {
			case NOT_STARTED:
				task.Status = IN_PROGRESS
				task.Category = REDUCE
				task.StartTime = time.Now()
				task.mu.Unlock()
				Debug.Printf("found not started reduce task %v", i)
				return task
			case IN_PROGRESS:
				// Check if task has timed out (more than 10 seconds)
				if time.Since(task.StartTime) > TaskTimeout {
					Debug.Printf("reduce task %v timed out, reassigning", i)
					task.Status = IN_PROGRESS // Reassign to new worker
					task.StartTime = time.Now()
					task.mu.Unlock()
					return task
				}
				// Task is still in progress and hasn't timed out
				task.mu.Unlock()
				continue
			case COMPLETED:
				completedReduceTasks++
				task.mu.Unlock()
			default:
				task.mu.Unlock()
				continue
			}
		}

		if len(m.reduceTasks) == completedReduceTasks {
			Debug.Printf("all reduce tasks completed")
			// all tasks are completed, so stop the program
			os.Exit(0)
		}
	}
	Debug.Printf("no not started tasks found")
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

	for i := range m.reduceTasks {
		task := &m.reduceTasks[i]
		task.mu.Lock()
		if task.Status != COMPLETED {
			task.mu.Unlock() // Must unlock before returning!
			return false
		}
		task.mu.Unlock()
	}

	for i := range m.mapTasks {
		task := &m.mapTasks[i]
		task.mu.Lock()
		if task.Status != COMPLETED {
			task.mu.Unlock() // Must unlock before returning!
			return false
		}
		task.mu.Unlock()
	}

	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := []Task{}
	for i, file := range files {
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
					Filename: "", // Reduce tasks don't need a filename - they read mr-X-Y files
					Status:   NOT_STARTED,
					TaskNo:   i,
				},
			},
		)
	}
	Debug.Printf("Created %v map tasks and %v reduce tasks", len(mapTasks), len(reduceTasks))

	m := Master{
		nReduce:     nReduce,
		nMap:        len(files),
		mapTasks:    mapTasks,
		reduceTasks: reduceTasks,
	}

	m.server()
	return &m
}
