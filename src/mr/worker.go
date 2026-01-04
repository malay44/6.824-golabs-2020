package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

var (
	// Loggers controlled by environment variables:
	// - MR_DEBUG: Set to "1" or "true" to enable debug logs (default: disabled)
	// - MR_INFO: Set to "0" or "false" to disable info logs (default: enabled)
	// - MR_ERROR: Set to "0" or "false" to disable error logs (default: enabled)
	Debug *log.Logger = initLogger("MR_DEBUG", os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile, true)
	Info  *log.Logger = initLogger("MR_INFO", os.Stdout, "INFO: ", log.Ldate|log.Ltime, false)
	Error *log.Logger = initLogger("MR_ERROR", os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile, false)
)

// initLogger creates a logger based on an environment variable
// envVar: name of the environment variable to check
// output: where to write logs (os.Stdout or os.Stderr)
// prefix: log message prefix
// flags: log flags (log.Ldate, log.Ltime, etc.)
// defaultDisabled: if true, logger is disabled by default (enabled when env var is set)
// if false, logger is enabled by default (disabled when env var is "0" or "false")
func initLogger(envVar string, output io.Writer, prefix string, flags int, defaultDisabled bool) *log.Logger {
	envValue := os.Getenv(envVar)

	var enabled bool
	if defaultDisabled {
		// For debug: enabled only if env var is set and not "0" or "false"
		enabled = envValue != "" && envValue != "0" && envValue != "false"
	} else {
		// For info/error: enabled by default, disabled only if env var is "0" or "false"
		enabled = envValue == "" || (envValue != "0" && envValue != "false")
	}

	if enabled {
		return log.New(output, prefix, flags)
	}
	// Logger disabled - use io.Discard to suppress output
	return log.New(io.Discard, "", 0)
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Create temp files and encoders for each reduce partition
type fileEncoder struct {
	file      *os.File
	enc       *json.Encoder
	tempName  string
	finalName string
}
type fes []*fileEncoder

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Clean up any files we've already created
func (fe fes) remove() {
	for _, fe := range fe {
		if fe != nil {
			fe.file.Close()
			os.Remove(fe.file.Name())
		}
	}
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapFn func(string, string) []KeyValue, reduceFn func(string, []string) string) {

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for true {
		reply, ok := CallGetTask()
		if !ok {
			Error.Printf("cannot get task")
			sleepRandom(rng, 1000, 3000)
			continue
		}
		taskInfo := reply.TaskInfo

		switch taskInfo.Category {
		case WAIT:
			Debug.Printf("got wait task")
			sleepRandom(rng, 1000, 3000)
			continue
		case MAP:
			err := handleMapTask(mapFn, taskInfo.Filename, taskInfo.TaskNo, reply.NReduce)
			if err != nil {
				Error.Printf("cannot write map output files: %v", err)
				continue
			}
			CallMarkTaskDone(taskInfo)
		case REDUCE:
			err := handleReduceTask(reduceFn, taskInfo.TaskNo, reply.NMap)
			if err != nil {
				Error.Printf("cannot process reduce task: %v", err)
				continue
			}
			success := CallMarkTaskDone(taskInfo)
			if success {
				cleanUpIntermediateFiles(taskInfo.TaskNo, reply.NMap)
			} else {
				Error.Printf("cannot mark reduce task as done")
			}
			continue
		}
	}
}

func sleepRandom(rng *rand.Rand, minMs, maxMs int64) {
	if minMs < 0 || maxMs < minMs {
		panic("invalid sleep bounds")
	}

	// Random duration in range
	sleepMs := minMs + rng.Int63n(maxMs-minMs+1)
	Debug.Printf("Sleeping for %v", sleepMs)
	time.Sleep(time.Duration(sleepMs) * time.Millisecond)
}

func handleMapTask(mapFn func(string, string) []KeyValue, filename string, mapTaskNo int, nReduce int) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("cannot open %v: %v", filename, err)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read %v: %v", filename, err)
	}
	kva := mapFn(filename, string(content))
	return divideKeysInBuckets(kva, mapTaskNo, nReduce)
}

func handleReduceTask(reduceFn func(string, []string) string, reduceTaskNo int, nMap int) error {
	// Read all mr-X-Y files where Y == reduceTaskNo
	intermediate := []KeyValue{}
	for mapTaskNo := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNo, reduceTaskNo)
		file, err := os.Open(filename)
		if err != nil {
			// File might not exist if map task produced no output for this reduce partition
			Debug.Printf("reduce task %d: map task %d produced no output (file %s not found)", reduceTaskNo, mapTaskNo, filename)
			continue
		}

		// Read key/value pairs using json.NewDecoder
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reduceTaskNo)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-Y.
	//
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
		output := reduceFn(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return nil
}

func cleanUpIntermediateFiles(reduceTaskNo int, nMap int) {
	// Read all mr-X-Y files where Y == reduceTaskNo
	for mapTaskNo := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNo, reduceTaskNo)
		os.Remove(filename)
		Debug.Printf("removed intermediate file %s", filename)
	}
}

func divideKeysInBuckets(kva []KeyValue, mapTaskNo int, nReduce int) error {
	if kva == nil {
		return fmt.Errorf("kva cannot be nil pointer")
	}

	files := make(fes, nReduce)

	// Initialize temp files and encoders for each reduce partition
	for reduceTaskNo := range nReduce {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNo, reduceTaskNo)
		tempFilename := filename + ".tmp"

		file, err := os.Create(tempFilename)
		if err != nil {
			files.remove()
			return fmt.Errorf("failed to create temp file %v: %v", tempFilename, err)
		}

		files[reduceTaskNo] = &fileEncoder{
			file:      file,
			enc:       json.NewEncoder(file),
			tempName:  tempFilename,
			finalName: filename,
		}
	}

	// Write each key/value pair incrementally to the appropriate file
	for _, kv := range kva {
		bucketNo := ihash(kv.Key) % nReduce
		if err := files[bucketNo].enc.Encode(&kv); err != nil {
			files.remove()
			return fmt.Errorf("failed to encode key/value pair: %v", err)
		}
	}

	// Close, sync, and atomically rename all files
	for _, fe := range files {
		if err := fe.file.Sync(); err != nil {
			files.remove()
			return fmt.Errorf("failed to sync file %v: %v", fe.tempName, err)
		}

		if err := fe.file.Close(); err != nil {
			files.remove()
			return fmt.Errorf("failed to close file %v: %v", fe.tempName, err)
		}

		if err := os.Rename(fe.tempName, fe.finalName); err != nil {
			files.remove()
			return fmt.Errorf("failed to rename %v to %v: %v", fe.tempName, fe.finalName, err)
		}

		Debug.Printf("Map task %d: wrote output to %v\n", mapTaskNo, fe.finalName)
	}

	return nil
}

func CallGetTask() (*GetTaskReply, bool) {
	args := EmptyArgs{}
	reply := GetTaskReply{}

	ok := call("Master.GetTask", &args, &reply)
	if !ok {
		return nil, false
	}
	return &reply, true
}

func CallMarkTaskDone(taskInfo TaskInfo) bool {
	args := MarkTaskDoneArgs{TaskInfo: taskInfo}
	reply := EmptyArgs{}

	return call("Master.MarkTaskDone", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		Info.Println("Worker terminating with log.Fatal")
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
