package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 1. 请求任务的参数（通常为空，Worker 只是问：能给我个活干吗？）
type RequestTaskArgs struct {
}

// 2. 响应任务的参数（Coordinator 告诉 Worker 具体做什么）
type RequestTaskReply struct {
	TaskType TaskType // 0: Map, 1: Reduce, 2: Wait, 3: Exit
	TaskID   int      // 任务编号
	FileName string   // 如果是 Map 任务，需要知道处理哪个文件
	NReduce  int      // 传入 Map 用于分桶，传入 Reduce 用于确定读取多少个中间文件
	NMap     int      // Map 任务总数
}

// 3. 汇报完成的参数（Worker 告诉 Coordinator 我做完了）
type TaskDoneArgs struct {
	TaskType TaskType
	TaskID   int
}

// 4. 汇报完成的响应（通常为空，Coordinator 收到即可）
type TaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
