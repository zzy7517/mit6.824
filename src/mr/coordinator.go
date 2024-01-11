package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	m             sync.Mutex
	fileNum       int            // file數量
	fileToMapTask map[string]int // 文件和任務序號對應關係
	mapTaskChan   chan string
	taskState     map[int]int // 任務狀態，等待執行、進行中、結束
	nReduce       int         // 所有的reduce任务
	reduceTasks   chan string
	phase         int
}

// 分配任务
func (c *Coordinator) coordinateTask(args *TaskArgs, reply *Task) error {
	if c.phase == mapping {
		file := <-c.mapTaskChan
		reply.fileName = file
		reply.taskId = c.fileToMapTask[file]
		fmt.Println("sending task " + strconv.Itoa(reply.taskId) + "file name is " + file)
		return nil
	} else if c.phase == reducing {
		return nil
	}
	return nil
}

func (c *Coordinator) getResult(args *Task, reply *Task) error {
	if c.phase == mapping {
		if reply.taskState == done {
			c.taskState[reply.taskId] = done
			c.checkMapDone()
		} else {
			c.taskState[reply.taskId] = waiting
			c.mapTaskChan <- reply.fileName
		}
	} else if c.phase == reducing {

	} else {

	}
	return nil
}

func (c *Coordinator) checkMapDone() {
	for _, v := range c.taskState {
		if v == waiting {
			return
		}
	}
	c.phase = reducing
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) generateMapTasks(files []string) {
	c.phase = mapping
	for n, f := range files {
		c.fileToMapTask[f] = n
		c.taskState[n] = waiting
		c.mapTaskChan <- f
	}
}
