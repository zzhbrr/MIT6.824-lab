package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task, nReduce, _ := ReqTask()
		fmt.Printf("pid %v get task %v\n", os.Getpid(), task)
		tmpfiles := RunFunc(&task, mapf, reducef, nReduce)
		fmt.Printf("pid %v finish task %v\n", os.Getpid(), task.TaskID)
		DeclFinish(task, tmpfiles)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func ReqTask() (Task, int, error) {
	args := ReqTaskArgs{}
	reply := ReqTaskReply{}
	ok := call("Coordinator.ReqTask", &args, &reply)
	if ok {
		return reply.AssignedTask, reply.NReduce, nil
	} else {
		fmt.Printf("%v call ReqTask failed!\n", os.Getegid())
		return Task{}, -1, nil
	}
}

func DeclFinish(task Task, tmpfiles []string) error {
	args := DeclFinishArgs{task.NowWorker, tmpfiles, task}
	reply := DeclFinishReply{}
	ok := call("Coordinator.DeclFinish", &args, &reply)
	if ok {
		return nil
	} else {
		fmt.Printf("%v call DeclFinish failed!\n", os.Getegid())
		return nil
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

func RunFunc(task *Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string, nReduce int) []string {
	tmpres := []string{}
	if task.TaskType == 0 { // map
		// assert(len(task.FileNames) == 1)
		file, err := os.Open(task.FileNames[0])
		if err != nil {
			log.Fatalf("cannot open %v", task.FileNames[0])
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.FileNames[0])
		}
		file.Close()
		kva := mapf(task.FileNames[0], string(content))
		sort.Sort(ByKey(kva))

		tmpfiles := []*os.File{}
		for i := 0; i < nReduce; i++ {
			tmpFileName := fmt.Sprintf("mr-%d-%d-", task.TaskID, i)
			tmpFile, err := ioutil.TempFile("", tmpFileName)
			if err != nil {
				log.Fatal("error: create temp output file failed.")
			}
			tmpfiles = append(tmpfiles, tmpFile)
			tmpres = append(tmpres, tmpFile.Name())
			// defer os.Remove(tmpFile.Name())
			fmt.Printf("Create tmpfile %v\n", tmpFile.Name())
		}
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			reduceID := ihash(kva[i].Key) % nReduce
			enc := json.NewEncoder(tmpfiles[reduceID])
			for k := i; k < j; k++ {
				enc.Encode(&kva[k])
			}
			i = j
		}
	} else if task.TaskType == 1 { // reduce
		kva := []KeyValue{}
		fmt.Printf("reduce %v get %v files\n", task.TaskID, len(task.FileNames))
		for _, filename := range task.FileNames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
		sort.Sort(ByKey(kva))
		tmpFileName := fmt.Sprintf("mr-out-%d-", task.TaskID-1)
		tmpFile, err := ioutil.TempFile("", tmpFileName)
		// fmt.Printf("Create tmpfile %v\n", tmpFile.Name())
		if err != nil {
			log.Fatal("error: create temp output file failed.")
		}
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			key := kva[i].Key
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(key, values)
			fmt.Fprintf(tmpFile, "%v %v\n", key, output)
			i = j
		}
		tmpres = append(tmpres, tmpFile.Name())
	} else if task.TaskType == 2 { // idle
		time.Sleep(3 * time.Second)
	} else { // exit
		os.Exit(0)
	}
	return tmpres
}
