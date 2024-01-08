package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files      []string
	fileNum    int            // file數量
	fileToTask map[string]int // 文件和任務序號對應關係
	taskState  map[int]int    // 任務狀態，等待執行、進行中、結束
}

// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) coordinateTask(args *TaskArgs, reply *Task) error {
	return nil
}

func (c *Coordinator) getResult(args *Task, reply *Task) error {
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
