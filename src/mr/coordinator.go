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

type Coordinator struct {
	// Your definitions here.
	tasks []Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) getNextPendingTask() Task {
	for idx, t := range c.tasks {
		t.mu.Lock()
		if t.Status == StatusPending {
			// NOTE: Marking task as in progress
			// till we either mark it as success or timeout (return it back to pending)
			c.tasks[idx].Status = StatusInProgress
			t.mu.Unlock()
			go func(idx int) {
				time.Sleep(time.Second * 10)
				c.tasks[idx].mu.Lock()
				defer c.tasks[idx].mu.Unlock()
				if c.tasks[idx].Status != StatusFinished {
					c.tasks[idx].Status = StatusPending
				}
			}(idx)
			return t
		}
		t.mu.Unlock()
	}
	return Task{
		TaskType: TypeExit,
	}
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	reply.Task = c.getNextPendingTask()
	return nil
}

func (c *Coordinator) MarkFinished(args *MarkFinishedArgs, reply *MarkFinishedReply) error {
	log.Printf("Task FINISHED: %d", args.Idx)
	c.tasks[args.Idx].mu.Lock()
	c.tasks[args.Idx].Status = StatusFinished
	c.tasks[args.Idx].mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	for _, t := range c.tasks {
		t.mu.Lock()
		if t.Status != StatusFinished {
			t.mu.Unlock()
			return false
		}
		t.mu.Unlock()
	}
	// return true if all tasks are finished
	return true
}

func (c *Coordinator) prepareTasks(files []string, nReduce int) {
	taskIdx := 0
	intermediateFilesPrefix := "intermediate"
	for fileIdx, f := range files {
		outputFiles := []string{}
		for reduceIdx := range nReduce {
			outputFiles = append(outputFiles, fmt.Sprintf("%s-%d-%d.txt", intermediateFilesPrefix, fileIdx, reduceIdx))
		}
		// 1 MAP Task for each input file
		c.tasks = append(c.tasks, Task{
			TaskType: TypeMap,
			Input:    []string{f},
			Output:   outputFiles,
			Status:   StatusPending,
			Idx:      taskIdx,
			mu:       &sync.Mutex{},
		})
		taskIdx++
	}
	// create nReduce REDUCE Task
	for reduceIdx := range nReduce {
		inputFiles := []string{}
		for fileIdx := range files {
			inputFiles = append(inputFiles, fmt.Sprintf("%s-%d-%d.txt", intermediateFilesPrefix, fileIdx, reduceIdx))
		}

		c.tasks = append(c.tasks, Task{
			TaskType: TypeReduce,
			Input:    inputFiles,
			Output:   []string{fmt.Sprintf("mr-out-%d.txt", reduceIdx)},
			Status:   StatusPending,
			Idx:      taskIdx,
			mu:       &sync.Mutex{},
		})
		taskIdx++

	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.prepareTasks(files, nReduce)

	c.server()
	return &c
}
