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
	// Create a specific logger for debug messages
	Debug *log.Logger = log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	Info  *log.Logger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime)
	Error *log.Logger = log.New(os.Stderr, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueFile struct {
	KeyValue
	filename string
}

// for sorting by key.
type ByKey []KeyValueFile

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	rng *rand.Rand
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
	
	w := worker{
		// Create per-call RNG (avoids contention & predictable sequences)
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	for true {
		w._worker(mapFn, reduceFn)
	}
}

func (w *worker) _worker(mapFn func(string, string) []KeyValue, reduceFn func(string, []string) string) {
	taskInfo, nReduce, nMap := CallGetTask()

	switch taskInfo.Category {
	case WAIT:
		Debug.Printf("got wait task")
		sleepRandom(w.rng, 200, 600)
		return;
	case MAP:
		err := handleMapTask(mapFn, taskInfo.Filename, taskInfo.TaskNo, nReduce)
		if err != nil {
			Error.Printf("cannot write map output files: %v", err)
			return
		}
		CallMarkTaskDone(*taskInfo)
	case REDUCE:
		err := handleReduceTask(reduceFn, taskInfo.TaskNo, nMap)
		if err != nil {
			Error.Printf("cannot process reduce task: %v", err)
			return
		}
		CallMarkTaskDone(*taskInfo)
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
	intermediate := []KeyValueFile{}
	for mapTaskNo := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNo, reduceTaskNo)
		file, err := os.Open(filename)
		if err != nil {
			// File might not exist if map task produced no output for this reduce partition
			Debug.Printf("reduce task %d: map task %d produced no output (file %s not found)", reduceTaskNo, mapTaskNo, filename)
			continue
		}

		content, err := io.ReadAll(file)
		file.Close() // Close immediately after reading
		if err != nil {
			return fmt.Errorf("cannot read %v: %v", filename, err)
		}

		kvf := decodeBucketContent(string(content))
		intermediate = append(intermediate, kvf...)
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

func decodeBucketContent(content string) []KeyValueFile {
	// Decode entire JSON array at once
	var kva []KeyValue
	if err := json.Unmarshal([]byte(content), &kva); err != nil {
		Debug.Printf("Failed to decode JSON array: %v\n", err)
		return []KeyValueFile{} // Return empty slice on error
	}

	// Convert []KeyValue to []KeyValueFile
	kvf := make([]KeyValueFile, len(kva))
	for i, kv := range kva {
		kvf[i] = KeyValueFile{
			KeyValue: kv,
			filename: "", // Not needed in new design
		}
	}
	return kvf
}

// divideKeysInBuckets writes map output to mr-X-Y files where X is the map task number
// and Y is the reduce partition number. Uses atomic writes (temp file + rename) for safety.
func divideKeysInBuckets(kva []KeyValue, mapTaskNo int, nReduce int) error {
	if kva == nil {
		return fmt.Errorf("kva cannot be nil pointer")
	}

	// Partition keys into reduce buckets
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucketNo := ihash(kv.Key) % nReduce
		buckets[bucketNo] = append(buckets[bucketNo], kv)
	}

	// Write each bucket to its own file atomically as a JSON array
	for reduceTaskNo, bucket := range buckets {
		if len(bucket) == 0 {
			Debug.Printf("Map task %d: Empty content for reduce partition %d\n", mapTaskNo, reduceTaskNo)
			continue
		}

		// Encode entire bucket as a JSON array
		jsonData, err := json.Marshal(bucket)
		if err != nil {
			return fmt.Errorf("failed to marshal bucket %d: %v", reduceTaskNo, err)
		}

		filename := fmt.Sprintf("mr-%d-%d", mapTaskNo, reduceTaskNo)
		err = writeFileAtomically(filename, string(jsonData))
		if err != nil {
			return fmt.Errorf("failed to write %v: %v", filename, err)
		}
		Debug.Printf("Map task %d: wrote output to %v\n", mapTaskNo, filename)
	}
	return nil
}

// writeFileAtomically writes content to filename atomically by:
// 1. Writing to a temporary file
// 2. Syncing the file to disk
// 3. Renaming the temp file to the final filename
// This ensures that readers either see the complete file or nothing (no partial writes).
func writeFileAtomically(filename, content string) error {
	// Create temporary file in the same directory
	tempFilename := filename + ".tmp"
	
	file, err := os.Create(tempFilename)
	if err != nil {
		return err
	}

	_, err = file.WriteString(content)
	if err != nil {
		file.Close()
		os.Remove(tempFilename)
		return err
	}

	// Sync to ensure data is written to disk before rename
	err = file.Sync()
	if err != nil {
		file.Close()
		os.Remove(tempFilename)
		return err
	}

	err = file.Close()
	if err != nil {
		os.Remove(tempFilename)
		return err
	}

	// Atomic rename: either succeeds completely or fails (no partial state)
	err = os.Rename(tempFilename, filename)
	if err != nil {
		os.Remove(tempFilename)
		return err
	}

	return nil
}

func CallGetTask() (*TaskInfo, int, int) {
	args := EmptyArgs{}
	reply := GetTaskReply{}

	call("Master.GetTask", &args, &reply)
	return &reply.TaskInfo, reply.NReduce, reply.NMap
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
