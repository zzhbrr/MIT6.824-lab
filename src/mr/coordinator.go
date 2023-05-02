package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	TaskID       int      // begin with 1, map and reduce task id are independent
	TaskType     int      // 0: map, 1: reduce, 2: idle, 3: exit
	FileNames    []string // input file names (map only one filename)
	NowWorker    int      // worker id, -1 means no worker
	FailedWorker []int    // faile workers id
	TaskState    int      // 0: not start, 1: running, 2: finished
	StartTime    int64
}

var idleTask = Task{-1, 2, []string{}, -1, nil, -1, 0}
var exitTask = Task{-1, 3, []string{}, -1, nil, -1, 0}

type Coordinator struct {
	// Your definitions here.
	NReduce     int
	NMap        int
	NumWorker   int
	Tasks       []Task
	TempFilePos [][]string // map output files
	FinalFiles  []string   // reduce output files
	State       int        // 0: mapping, 1: reducing, 2: finished
	Mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	reply.NReduce = c.NReduce
	if c.State == 0 { // assign map task
		for i, task := range c.Tasks {
			if task.TaskType != 0 || task.TaskState != 0 {
				continue
			}
			c.Tasks[i].TaskState = 1
			c.NumWorker++
			c.Tasks[i].NowWorker = c.NumWorker
			c.Tasks[i].StartTime = time.Now().Unix()
			reply.AssignedTask = c.Tasks[i]
			fmt.Printf("assign map task %d to worker %d\n", c.Tasks[i].TaskID, c.Tasks[i].NowWorker)
			return nil
		}
		reply.AssignedTask = idleTask // no map task, idle
	} else if c.State == 1 { // assign reduce task
		for i, task := range c.Tasks {
			if task.TaskType != 1 || task.TaskState != 0 {
				continue
			}
			c.Tasks[i].TaskState = 1
			c.NumWorker++
			c.Tasks[i].NowWorker = c.NumWorker
			c.Tasks[i].StartTime = time.Now().Unix()
			reply.AssignedTask = c.Tasks[i]
			fmt.Printf("assign reduce task %d to worker %d\n", c.Tasks[i].TaskID, c.Tasks[i].NowWorker)
			return nil
		}
		reply.AssignedTask = idleTask // no reduce task, idle
	} else { // assign exit task
		reply.AssignedTask = exitTask // exit
	}
	return nil
}

type ByStringValue []string

func (a ByStringValue) Len() int           { return len(a) }
func (a ByStringValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByStringValue) Less(i, j int) bool { return a[i] < a[j] }
func (c *Coordinator) DeclFinish(args *DeclFinishArgs, reply *DeclFinishReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	fmt.Printf("worker %d declare finished task %d with type %d\n", args.WorkerID, args.FinishedTask.TaskID, args.FinishedTask.TaskType)
	if args.FinishedTask.TaskID == -1 { // worker exit or idle, do nothing
		return nil
	}
	if (args.FinishedTask.TaskType == 0 && args.WorkerID != c.Tasks[args.FinishedTask.TaskID-1].NowWorker) ||
		(args.FinishedTask.TaskType == 1 && args.WorkerID != c.Tasks[args.FinishedTask.TaskID+c.NMap-1].NowWorker) {
		// 提交任务的worker不是当前任务指定的worker, 当前worker太慢, 任务已经被分配给了其他worker
		return nil
	}
	// 可以接收
	if args.FinishedTask.TaskType == 0 { // map task
		c.TempFilePos[args.FinishedTask.TaskID-1] = args.TempFiles
		c.Tasks[args.FinishedTask.TaskID-1].TaskState = 2

		// 将map输出文件名按字典序排序, 放到reduce任务的输入文件名列表中
		// sort.Sort(ByStringValue(c.Tasks[args.FinishedTask.TaskID-1].FileNames))
		for i, task := range c.Tasks {
			if task.TaskType == 1 {
				c.Tasks[i].FileNames = append(c.Tasks[i].FileNames, args.TempFiles[i-c.NMap])
			}
		}
		fmt.Printf("map task %d finished\n", args.FinishedTask.TaskID)

		// check if all map tasks are finished
		for _, task := range c.Tasks {
			if task.TaskType == 0 && task.TaskState != 2 {
				return nil
			}
		}
		// all map tasks are finished
		c.State = 1
		fmt.Printf("all map tasks finished\n")
	} else if args.FinishedTask.TaskType == 1 { // reduce task
		assert(c.State == 1)
		finalname := fmt.Sprintf("mr-out-%d", args.FinishedTask.TaskID-1)
		// fmt.Printf("reduce task %d rename %s as output file: %s\n", args.FinishedTask.TaskID, args.TempFiles[0], finalname)
		os.Rename(args.TempFiles[0], finalname)
		c.FinalFiles = append(c.FinalFiles, finalname)
		c.Tasks[args.FinishedTask.TaskID+c.NMap-1].TaskState = 2
		fmt.Printf("reduce task %d finished\n", args.FinishedTask.TaskID)
		// check if all reduce tasks are finished
		for _, task := range c.Tasks {
			if task.TaskType == 1 && task.TaskState != 2 {
				return nil
			}
		}
		// all reduce tasks are finished
		c.State = 2
		fmt.Printf("all reduce tasks finished\n")
	}

	return nil
}

func assert(b bool) {
	if !b {
		panic("assert error")
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// time.Sleep(1 * time.Second)
	// time.Sleep(1 * time.Second)
	// time.Sleep(1 * time.Second)
	// return true
	ret := false
	if c.State == 2 {
		ret = true
		dir, err := os.Open("/tmp")
		if err != nil {
			fmt.Printf("open /tmp error: %v\n", err)
		}
		defer dir.Close()
		files, err := dir.Readdir(-1)
		if err != nil {
			fmt.Printf("read dir error: %v\n", err)
		}
		for _, file := range files {
			if file.Name()[:3] == "mr-" {
				os.Remove("/tmp/" + file.Name())
			}
		}

	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	initCoordinator(&c, files, nReduce)
	// fmt.Printf("Coordinator init finish: %v\n", c)
	fmt.Printf("Coordinator init finish %v %v %v %v %v %v %v\n", c.NReduce, c.NMap, c.NumWorker, c.Tasks, c.TempFilePos, c.FinalFiles, c.State)

	go CrashDetection(&c)

	c.server()
	return &c
}

func initCoordinator(c *Coordinator, files []string, nReduce int) {
	// init coordinator
	for _, filename := range files {
		c.NMap++
		c.Tasks = append(c.Tasks, Task{c.NMap, 0, []string{filename}, -1, []int{}, 0, 0})
	}
	c.NReduce = nReduce
	for i := 1; i <= nReduce; i++ {
		c.Tasks = append(c.Tasks, Task{i, 1, []string{}, -1, []int{}, 0, 0})
	}
	c.TempFilePos = make([][]string, c.NMap)
	c.FinalFiles = make([]string, c.NReduce)
}

func CrashDetection(c *Coordinator) {
	for {
		c.Mu.Lock()
		for i, task := range c.Tasks {
			if task.TaskState == 1 {
				if time.Now().Unix()-task.StartTime > 10 {
					fmt.Printf("worker %d on task %d timeout, reassign\n", task.NowWorker, task.TaskID)
					c.Tasks[i].TaskState = 0
					c.Tasks[i].FailedWorker = append(c.Tasks[i].FailedWorker, task.NowWorker)
				}
			}
		}
		c.Mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}
