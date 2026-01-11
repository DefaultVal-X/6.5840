package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 任务类型和状态的常量
type TaskType int
type TaskStatus int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // 现在没任务，但以后可能有
	ExitTask // 以后都不会有新的任务了
)

// 空闲、进行中、已完成任务状态
const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// 任务元数据结构体
type TaskMetaData struct {
	ID        int
	Type      TaskType
	Status    TaskStatus
	FileName  string
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	// 任务队列
	mapTasks    []TaskMetaData
	reduceTasks []TaskMetaData

	// 任务数量
	nMap    int
	nReduce int

	phase TaskType // 当前阶段：Map或Reduce
}

// Your code here -- RPC handlers for the worker to call.
// 1. Worker 调用此 RPC 获取任务
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 根据当前阶段分配任务
	var tasks *[]TaskMetaData
	if c.phase == MapTask {
		tasks = &c.mapTasks
	} else {
		tasks = &c.reduceTasks
	}

	// 遍历任务队列，寻找空闲任务
	allDone := true
	for i := range *tasks {
		if (*tasks)[i].Status == Completed {
			continue
		}
		allDone = false
		if (*tasks)[i].Status == Idle {
			// 分配任务给worker
			(*tasks)[i].Status = InProgress
			(*tasks)[i].StartTime = time.Now()
			reply.TaskType = (*tasks)[i].Type
			reply.TaskID = (*tasks)[i].ID
			reply.FileName = (*tasks)[i].FileName
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			return nil
		}
	}

	if !allDone {
		// 如果有任务正在进行中，要求worker等待
		reply.TaskType = WaitTask
	} else {
		// 如果 Map 阶段全做完了，切换到 Reduce 阶段
		if c.phase == MapTask {
			c.phase = ReduceTask
			reply.TaskType = WaitTask
		} else {
			// 如果 Reduce 阶段也全做完了，通知 worker 退出
			reply.TaskType = ExitTask
		}
	}
	return nil

}

// 2. Worker 调用此 RPC 上报任务完成情况
func (c *Coordinator) ReportTaskCompletion(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 根据当前阶段更新任务状态
	var tasks *[]TaskMetaData
	if args.TaskType == MapTask {
		tasks = &c.mapTasks
	} else {
		tasks = &c.reduceTasks
	}

	// 标记任务为已完成
	if args.TaskID < len(*tasks) && (*tasks)[args.TaskID].Status == InProgress {
		(*tasks)[args.TaskID].Status = Completed
	}

	return nil
}

// 3. 周期性检查超时的协程
func (c *Coordinator) catchTimeouts() {
	for {
		time.Sleep(2 * time.Second)
		c.mu.Lock()

		var tasks *[]TaskMetaData
		if c.phase == MapTask {
			tasks = &c.mapTasks
		} else {
			tasks = &c.reduceTasks
		}

		for i := range *tasks {
			if (*tasks)[i].Status == InProgress && time.Since((*tasks)[i].StartTime) > 10*time.Second {
				log.Printf("Task %d timed out, resetting to Idle", (*tasks)[i].ID)
				(*tasks)[i].Status = Idle
			}
		}
		c.mu.Unlock()
	}
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
	// Your code here.
	ret := true
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase != ReduceTask {
		ret = false
	}
	// 检查所有任务是否都已完成
	for _, task := range c.reduceTasks {
		if task.Status != Completed {
			ret = false
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		phase:       MapTask,
		mapTasks:    make([]TaskMetaData, len(files)),
		reduceTasks: make([]TaskMetaData, nReduce),
	}

	// 初始化 Map 任务
	for i, file := range files {
		c.mapTasks[i] = TaskMetaData{
			ID:       i,
			Type:     MapTask,
			Status:   Idle,
			FileName: file,
		}
	}

	// 初始化 Reduce 任务
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = TaskMetaData{
			ID:     i,
			Type:   ReduceTask,
			Status: Idle,
		}
	}

	// 启动超时检查协程
	go c.catchTimeouts()

	c.server()
	return &c
}
