package coordinator

import (
	"6.5840/mr"
	"fmt"
	"strconv"
)

type reduceCoordinator struct {
	nReduce         int
	reduceTaskMap   map[int][]string // reduce number下对应的所有interfile
	reduceTaskChan  chan int
	reduceTaskState map[int]mr.TaskStateEnum
}

func (r *reduceCoordinator) coordinateTask(args *mr.TaskArgs, reply *mr.Task) error {
	taskId := <-r.reduceTaskChan
	interFiles := r.reduceTaskMap[taskId]
	reply.InterFiles = interFiles
	reply.TaskId = taskId
	fmt.Println("sending reduce task " + strconv.Itoa(reply.TaskId) + "reduce number is " + strconv.Itoa(taskId))
	return nil
}

func (r *reduceCoordinator) getResult(args *mr.Task, reply *mr.Task) bool {
	if reply.TaskState == mr.TaskFinished {
		r.reduceTaskState[reply.TaskId] = mr.TaskFinished
		return true
	} else {
		r.reduceTaskChan <- reply.TaskId
		return false
	}
}

func (r *reduceCoordinator) checkReduceDone() bool {
	for _, v := range r.reduceTaskState {
		if v == mr.TaskWaiting {
			return false
		}
	}
	return true
}

func (r *reduceCoordinator) generateReduceTasks() {
	for k, _ := range r.reduceTaskMap {
		r.reduceTaskState[k] = mr.TaskWaiting
		r.reduceTaskChan <- k
	}
}
