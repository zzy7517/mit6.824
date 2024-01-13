package coordinator

import (
	"6.5840/mr"
	"fmt"
	"os"
	"strconv"
)

type MapCoordinator struct {
	mapTaskMap   map[int]string // map任务编号和文件名对应
	mapTaskChan  chan int
	mapTaskState map[int]mr.TaskStateEnum // 任務狀態: waiting/processing/finished
}

func (m *MapCoordinator) coordinateTask(args *mr.TaskArgs, reply *mr.Task) error {
	mapTaskNum := <-m.mapTaskChan
	reply.MapFileName = m.mapTaskMap[mapTaskNum]
	reply.TaskId = mapTaskNum
	fmt.Println("sending map task " + strconv.Itoa(reply.TaskId) + "file name is " + reply.MapFileName)
	return nil
}

func (m *MapCoordinator) getResult(args *mr.Task, reply *mr.Task) bool {
	if reply.TaskState == mr.TaskFinished {
		// map执行完成后，
		m.mapTaskState[reply.TaskId] = mr.TaskFinished
		return true
	} else {
		// map执行超时，重置这个file的状态，并且删除这个map执行过程中生成的中间文件
		m.mapTaskState[reply.TaskId] = mr.TaskWaiting
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

func (m *MapCoordinator) checkMapDone() bool {
	for _, v := range m.mapTaskState {
		if v == mr.TaskWaiting {
			return false
		}
	}
	return true
}

func (m *MapCoordinator) generateMapTasks(files []string) {
	for n, f := range files {
		m.mapTaskMap[n] = f
		m.mapTaskState[n] = mr.TaskWaiting
		m.mapTaskChan <- n
	}
}
