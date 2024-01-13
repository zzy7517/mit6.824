package coordinator

import (
	"6.5840/mr"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	phase             mr.PhaseEnum // 当前phase: mapping/reducing/done
	mapCoordinator    MapCoordinator
	reduceCoordinator reduceCoordinator
	result            []mr.KeyValue
	done              bool
}

// 分配任务
func (c *Coordinator) coordinateTask(args *mr.TaskArgs, reply *mr.Task) error {
	if c.phase == mr.Mapping {
		c.mapCoordinator.coordinateTask(args, reply)
	} else {
		c.reduceCoordinator.coordinateTask(args, reply)
	}
	return nil
}

func (c *Coordinator) getResult(args *mr.Task, reply *mr.Task) {
	if c.phase == mr.Mapping {
		isSucc := c.mapCoordinator.getResult(args, reply)
		if isSucc {
			for k, v := range reply.ReduceFileMap {
				c.reduceCoordinator.reduceTaskMap[k] = append(c.reduceCoordinator.reduceTaskMap[k], v...)
			}
			if c.mapCoordinator.checkMapDone() {
				c.phase = mr.Reducing
				c.reduceCoordinator.generateReduceTasks()
			}
		}
	} else if c.phase == mr.Reducing {
		isSucc := c.reduceCoordinator.getResult(args, reply)
		if isSucc {
			c.result = append(c.result, reply.ReduceResult...)
			if c.reduceCoordinator.checkReduceDone() {
				c.phase = mr.Done
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
	sockname := mr.CoordinatorSock()
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
	c.mapCoordinator.generateMapTasks(files)
	// Your code here.

	c.server()
	return &c
}
