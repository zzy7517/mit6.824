package mr

import (
	"fmt"
	"strconv"
)

type reduceCoordinator struct {
	nReduce         int
	reduceTaskMap   map[int][]string // reduce number下对应的所有interfile
	reduceTaskChan  chan int
	reduceTaskState map[int]TaskStateEnum
}

func (r *reduceCoordinator) coordinateTask(args *TaskArgs, reply *Task) error {
	taskId := <-r.reduceTaskChan
	interFiles := r.reduceTaskMap[taskId]
	reply.InterFiles = interFiles
	reply.TaskId = taskId
	fmt.Println("sending reduce task " + strconv.Itoa(reply.TaskId) + "reduce number is " + strconv.Itoa(taskId))
	return nil
}

func (r *reduceCoordinator) getResult(args *Task, reply *Task) bool {
	if reply.TaskState == TaskFinished {
		r.reduceTaskState[reply.TaskId] = TaskFinished
		return true
	} else {
		r.reduceTaskChan <- reply.TaskId
		return false
	}
}

func (r *reduceCoordinator) checkReduceDone() bool {
	for _, v := range r.reduceTaskState {
		if v == TaskWaiting {
			return false
		}
	}
	return true
}

func (r *reduceCoordinator) generateReduceTasks() {
	for k, _ := range r.reduceTaskMap {
		r.reduceTaskState[k] = TaskWaiting
		r.reduceTaskChan <- k
	}
}
