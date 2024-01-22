package mr

import (
	"fmt"
	"os"
	"strconv"
)

type mapCoordinator struct {
	mapTaskMap   map[int]string // map任务编号和文件名对应
	mapTaskChan  chan int
	mapTaskState map[int]TaskStateEnum // 任務狀態: waiting/processing/finished
	nReduce      int
}

func (m *mapCoordinator) coordinateTask(args *TaskArgs, reply *Task) error {
	mapTaskNum := <-m.mapTaskChan
	reply.MapFileName = m.mapTaskMap[mapTaskNum]
	reply.TaskId = mapTaskNum
	reply.nReduce = m.nReduce
	fmt.Printf("sending task is %v \n", reply)
	fmt.Println("sending map task " + strconv.Itoa(reply.TaskId) + "file name is " + reply.MapFileName)
	return nil
}

func (m *mapCoordinator) getResult(args *Task, reply *Task) bool {
	if reply.TaskState == TaskFinished {
		// map执行完成后，
		m.mapTaskState[reply.TaskId] = TaskFinished
		return true
	} else {
		// map执行超时，重置这个file的状态，并且删除这个map执行过程中生成的中间文件
		m.mapTaskState[reply.TaskId] = TaskWaiting
		for _, files := range reply.ReduceFileMap {
			for _, file := range files {
				err := os.Remove(file)
				if err != nil {
					fmt.Printf("remove file failed, err is %v \n", err)
				}
			}
		}
		// map执行超时，重新放入channel
		m.mapTaskChan <- reply.TaskId
		return false
	}
}

func (m *mapCoordinator) checkMapDone() bool {
	for _, v := range m.mapTaskState {
		if v == TaskWaiting {
			return false
		}
	}
	return true
}

func (m *mapCoordinator) generateMapTasks(files []string) {
	for n, f := range files {
		m.mapTaskMap[n] = f
		m.mapTaskState[n] = TaskWaiting
		m.mapTaskChan <- n
	}
}
