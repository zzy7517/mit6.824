package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

const timeout = 10 * time.Second

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	task, ok := getTask()
	fmt.Println("n reduce is " + strconv.Itoa(task.nReduce))
	if ok {
		// 10s超时
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		go doTask(task, mapf, reducef)
		select {
		case <-ctx.Done():
			fmt.Println("task finished, file name is %v", task.MapFileName)
			sendResult(task, true)
		case <-time.After(timeout):
			fmt.Printf("timeout!!!, file name is %v", task.MapFileName)
			sendResult(task, false)
			return
		}
	} else {
		fmt.Println("get task failed")
		sendResult(task, false)
	}
}

func getTask() (*Task, bool) {
	taskArgs := &TaskArgs{}
	reply := &Task{}
	ok := call("Coordinator.CoordinateTask", &taskArgs, reply)
	fmt.Printf("reply is %v \n", reply)
	return reply, ok
}

func doTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerType := task.workerType
	switch workerType {
	case workerMap:
		// 使用select控制worker超时
		doMapTask(task, mapf)
	case workerReduce:
		doReduceTask(task, reducef)
	default:
		return
	}
}

func doMapTask(task *Task, mapf func(string, string) []KeyValue) {
	filename := task.MapFileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	inters := mapf(filename, string(content))

	keyValueSplit := make(map[int][]KeyValue)

	// split map task
	// naming convention for intermediate files is mr-X-Y
	// where X is the Map task number, and Y is the reduce task number.
	for _, inter := range inters {
		hashKey := ihash(inter.Key) % task.nReduce
		keyValueSplit[hashKey] = append(keyValueSplit[hashKey], inter)
	}

	for k, v := range keyValueSplit {
		oname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(k)
		ofile, _ := os.Create(oname)
		//i := 0
		//for i < len(v) {
		//	fmt.Fprintf(ofile, "%v %v\n", v[i].Key, v[i].Value)
		//}
		enc := json.NewEncoder(ofile)
		for _, kv := range v {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Printf("encode kv to file %v failed\n", oname)
			}
		}
		ofile.Close()
		fmt.Printf("finish map task %v-%v", task.TaskId, k)
		task.ReduceFileMap = make(map[int][]string)
		task.ReduceFileMap[k] = append(task.ReduceFileMap[k], oname)
	}
}

func sendResult(task *Task, isFinished bool) {
	if isFinished {
		task.TaskState = TaskFinished
	} else {
		task.TaskState = TaskWaiting
	}
	reply := Task{}
	ok := call("Coordinator.GetResult", task, &reply)
	if ok {
		fmt.Println("send result succeed")
	} else {
		fmt.Println("send result to coordinator failed")
	}
}

func doReduceTask(task *Task, reducef func(string, []string) string) {
	interFiles := task.InterFiles
	var intermediate []KeyValue
	for _, fileName := range interFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", task.MapFileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	var reduceResult []KeyValue
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		reduceResult = append(reduceResult, KeyValue{
			Key:   intermediate[i].Key,
			Value: output,
		})
		i = j
	}
	task.ReduceResult = reduceResult
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := CoordinatorSock()
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
