package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskArgs struct {
}

type workerTypeEnum int

const (
	workerMap    workerTypeEnum = 0
	workerReduce workerTypeEnum = 1
	// when there is no task to handle
	workerNone workerTypeEnum = 2
)

type TaskStateEnum int

const (
	TaskWaiting    TaskStateEnum = 0
	taskProcessing TaskStateEnum = 1
	TaskFinished   TaskStateEnum = 2
)

type PhaseEnum int

const (
	Mapping  PhaseEnum = 0
	Reducing PhaseEnum = 1
	Done     PhaseEnum = 2
)

type Task struct {
	WorkerType    workerTypeEnum
	MapFileName   string
	TaskId        int // map number or reduce number
	TaskState     TaskStateEnum
	ReduceFileMap map[int][]string
	NReduce       int
	InterFiles    []string
	ReduceResult  []KeyValue
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
