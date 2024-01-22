package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	phase             PhaseEnum // 当前phase: mapping/reducing/done
	mapCoordinator    mapCoordinator
	reduceCoordinator reduceCoordinator
	result            []KeyValue
	done              bool
}

func (c *Coordinator) init(nReduce int) {
	c.mapCoordinator = mapCoordinator{
		mapTaskMap:   make(map[int]string),
		mapTaskChan:  make(chan int),
		mapTaskState: make(map[int]TaskStateEnum),
		nReduce:      nReduce,
	}

	c.reduceCoordinator = reduceCoordinator{
		reduceTaskMap:   make(map[int][]string),
		reduceTaskChan:  make(chan int),
		reduceTaskState: make(map[int]TaskStateEnum),
		nReduce:         nReduce,
	}
}

// 分配任务
func (c *Coordinator) CoordinateTask(args *TaskArgs, reply *Task) error {
	if c.phase == Mapping {
		err := c.mapCoordinator.coordinateTask(args, reply)
		if err != nil {
			fmt.Println("map coordinate task err: ", err)
			return err
		}
	} else {
		err := c.reduceCoordinator.coordinateTask(args, reply)
		if err != nil {
			fmt.Println("reduce coordinate task err: ", err)
			return err
		}
	}
	return nil
}

func (c *Coordinator) GetResult(args *Task, reply *Task) {
	if c.phase == Mapping {
		isSucc := c.mapCoordinator.getResult(args, reply)
		if isSucc {
			for k, v := range reply.ReduceFileMap {
				c.reduceCoordinator.reduceTaskMap[k] = append(c.reduceCoordinator.reduceTaskMap[k], v...)
			}
			if c.mapCoordinator.checkMapDone() {
				c.phase = Reducing
				c.reduceCoordinator.generateReduceTasks()
			}
		}
	} else if c.phase == Reducing {
		isSucc := c.reduceCoordinator.getResult(args, reply)
		if isSucc {
			c.result = append(c.result, reply.ReduceResult...)
			if c.reduceCoordinator.checkReduceDone() {
				c.phase = Done
				c.done = true
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
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
	c := Coordinator{}
	c.init(nReduce)
	go func() {
		c.mapCoordinator.generateMapTasks(files)
	}()
	// Your code here.

	c.server()
	return &c
}
