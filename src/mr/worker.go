package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 为排序定义的类型
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// 1. 请求任务
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatalf("Worker: RPC call to RequestTask failed")
			return
		}

		// 2. 根据任务类型执行不同逻辑
		switch reply.TaskType {
		case MapTask:
			// 执行 Map 任务
			doMap(mapf, reply)
			reportDone(MapTask, reply.TaskID)
		case ReduceTask:
			// 执行 Reduce 任务
			doReduce(reducef, reply)
			reportDone(ReduceTask, reply.TaskID)
		case WaitTask:
			// 等待一段时间再请求任务
			time.Sleep(time.Second)
		case ExitTask:
			return
		default:
			//log.Fatalf("Worker: Unknown TaskType %v", reply.TaskType)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// --- 处理 Map 任务 ---
func doMap(mapf func(string, string) []KeyValue, reply RequestTaskReply) {
	// 读取输入文件
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("Worker: cannot open %v", reply.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker: cannot read %v", reply.FileName)
	}
	file.Close()

	// 调用用户定义的 Map 函数
	kva := mapf(reply.FileName, string(content))

	// 将结果分桶，准备写入 NReduce 个中间文件
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	// 将每个桶写入对应的中间文件
	for i := 0; i < reply.NReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
		tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-tmp-%d-*", reply.TaskID))
		if err != nil {
			log.Fatalf("Worker: cannot create temp file for %v", intermediateFileName)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("Worker: cannot encode kv pair %v", kv)
			}
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), intermediateFileName)
	}
}

// --- 处理 Reduce 任务 ---
func doReduce(reducef func(string, []string) string, reply RequestTaskReply) {
	// 读取所有 Map 产生的针对该 Reduce 任务的中间文件
	// 中间文件名格式为 mr-X-Y，其中 X 是 Map 任务号，Y 是 Reduce 任务号
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("Worker: cannot open %v", intermediateFileName)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()

	}

	// 对中间结果进行排序
	sort.Sort(ByKey(intermediate))

	// 调用用户定义的 Reduce 函数
	outputFileName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	outputFile, err := os.CreateTemp("", "mr-tmp-*")
	if err != nil {
		log.Fatalf("Worker: cannot create output file %v", outputFileName)
	}
	// defer outputFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 写入输出文件
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	outputFile.Close()
	os.Rename(outputFile.Name(), outputFileName)
}

// 汇报完成
func reportDone(t TaskType, id int) {
	args := TaskDoneArgs{
		TaskType: t,
		TaskID:   id,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.ReportTaskCompletion", &args, &reply)
	if !ok {
		log.Fatalf("Worker: RPC call to ReportTaskCompletion failed")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
