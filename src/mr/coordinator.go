package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Coordinator struct {
	phase   PhaseEnum // 当前phase: mapping/reducing/done
	result  []KeyValue
	done    bool
	nReduce int

	mapTaskMap   map[int]string // map任务编号和文件名对应
	mapTaskChan  chan int
	mapTaskState map[int]TaskStateEnum // 任務狀態: waiting/processing/finished

	reduceTaskMap   map[int][]string // reduce number下对应的所有interfile
	reduceTaskChan  chan int
	reduceTaskState map[int]TaskStateEnum
}

// 分配任务
func (c *Coordinator) CoordinateTask(args *TaskArgs, reply *Task) error {
	if c.phase == Mapping {
		mapTaskNum := <-c.mapTaskChan
		reply.MapFileName = c.mapTaskMap[mapTaskNum]
		reply.TaskId = mapTaskNum
		reply.NReduce = c.nReduce
		fmt.Printf("sending task is %+v \n", reply)
		fmt.Println("sending map task " + strconv.Itoa(reply.TaskId) + " file name is " + reply.MapFileName)
		return nil
	} else {
		taskId := <-c.reduceTaskChan
		interFiles := c.reduceTaskMap[taskId]
		reply.InterFiles = interFiles
		reply.TaskId = taskId
		fmt.Println("sending reduce task " + strconv.Itoa(reply.TaskId) + "reduce number is " + strconv.Itoa(taskId))
		return nil
	}
}

func (c *Coordinator) GetResult(args *Task, reply *Task) {
	if c.phase == Mapping {
		isSucc := c.getMapResult(args, reply)
		if isSucc {
			for k, v := range reply.ReduceFileMap {
				c.reduceTaskMap[k] = append(c.reduceTaskMap[k], v...)
			}
			if c.checkMapDone() {
				c.phase = Reducing
				c.generateReduceTasks()
			}
		}
	} else if c.phase == Reducing {
		isSucc := c.getReduceResult(args, reply)
		if isSucc {
			c.result = append(c.result, reply.ReduceResult...)
			if c.checkReduceDone() {
				c.phase = Done
				c.done = true
			}
		}
	}
}

func (c *Coordinator) checkMapDone() bool {
	for _, v := range c.mapTaskState {
		if v == TaskWaiting {
			return false
		}
	}
	return true
}

func (c *Coordinator) getMapResult(args *Task, reply *Task) bool {
	if reply.TaskState == TaskFinished {
		// map执行完成后，
		c.mapTaskState[reply.TaskId] = TaskFinished
		return true
	} else {
		// map执行超时，重置这个file的状态，并且删除这个map执行过程中生成的中间文件
		c.mapTaskState[reply.TaskId] = TaskWaiting
		for _, files := range reply.ReduceFileMap {
			for _, file := range files {
				err := os.Remove(file)
				if err != nil {
					fmt.Printf("remove file failed, err is %v \n", err)
				}
			}
		}
		// map执行超时，重新放入channel
		c.mapTaskChan <- reply.TaskId
		return false
	}
}

func (c *Coordinator) getReduceResult(args *Task, reply *Task) bool {
	if reply.TaskState == TaskFinished {
		c.reduceTaskState[reply.TaskId] = TaskFinished
		return true
	} else {
		c.reduceTaskChan <- reply.TaskId
		return false
	}
}

func (c *Coordinator) checkReduceDone() bool {
	for _, v := range c.reduceTaskState {
		if v == TaskWaiting {
			return false
		}
	}
	return true
}

func (c *Coordinator) generateMapTasks(files []string) {
	for n, f := range files {
		c.mapTaskMap[n] = f
		c.mapTaskState[n] = TaskWaiting
		c.mapTaskChan <- n
	}
}

func (c *Coordinator) generateReduceTasks() {
	for k, _ := range c.reduceTaskMap {
		c.reduceTaskState[k] = TaskWaiting
		c.reduceTaskChan <- k
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		fmt.Printf("register err: %v \n", err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := CoordinatorSock()
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
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:         nReduce,
		mapTaskMap:      make(map[int]string),
		mapTaskChan:     make(chan int),
		mapTaskState:    make(map[int]TaskStateEnum),
		reduceTaskMap:   make(map[int][]string),
		reduceTaskChan:  make(chan int),
		reduceTaskState: make(map[int]TaskStateEnum),
	}
	fmt.Printf("init coordinator finished, %v \n", c)
	go func() {
		c.generateMapTasks(files)
	}()
	// Your code here.

	c.server()
	return &c
}
