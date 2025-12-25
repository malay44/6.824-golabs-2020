package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapFn func(string, string) []KeyValue, reduceFn func(string, []string) string) {
	for true {
		_worker(mapFn, reduceFn)
	}
}

func _worker(mapFn func(string, string) []KeyValue, reduceFn func(string, []string) string) {
	taskInfo, nReduce := CallGetTask()

	filename := taskInfo.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	switch taskInfo.Category {
	case MAP:
		err = handleMapTask(mapFn, filename, string(content), nReduce)
	case REDUCE:
		err = handleReduceTask(reduceFn, taskInfo.TaskNo, string(content))
	case WAIT:
		Debug.Printf("got wait task")
	}

	if err != nil {
		log.Fatalf("cannot append kva into bucket files: %v", err)
	}
	CallMarkTaskDone(*taskInfo)
}

func handleMapTask(mapFn func(string, string) []KeyValue, filename string, content string, nReduce int) error {
	kva := mapFn(filename, string(content))
	return divideKeysInBuckets(kva, filename, nReduce)
}

func handleReduceTask(reduceFn func(string, []string) string, taskNo int, content string) error {
	intermediate := decodeBucketContent(content)
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(taskNo)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
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

	ofile.Close()

	return nil
}

func decodeBucketContent(content string) []KeyValueFile {
	kvf := []KeyValueFile{}
	lines := strings.SplitSeq(content, "\n")

	for line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue // skip malformed / empty lines
		}

		kvf = append(kvf, KeyValueFile{
			KeyValue: KeyValue{
				Key:   fields[0],
				Value: fields[1],
			},
			filename: fields[2],
		})
	}
	return kvf
}

func divideKeysInBuckets(kva []KeyValue, filename string, nReduce int) error {
	if kva != nil {
		intermediateContent := make([]string, nReduce)
		for _, kv := range kva {
			bucketNo := ihash(kv.Key) % nReduce
			intermediateContent[bucketNo] += fmt.Sprintf("%v %v %v\n", kv.Key, kv.Value, filename)
		}
		for i, bucketContent := range intermediateContent {
			err := appendToFile("r-in-"+strconv.Itoa(i), bucketContent)
			if err != nil {
				return err
			}
			Debug.Printf("appended content for bucket %v\n", i)
		}
	} else {
		return fmt.Errorf("kva cannot be nil pointer")
	}
	return nil
}

func appendToFile(filename, text string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(text)
	return err
}

func CallGetTask() (*TaskInfo, int) {
	args := EmptyArgs{}
	reply := GetTaskReply{}

	call("Master.GetTask", &args, &reply)
	return &reply.TaskInfo, reply.NReduce
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
