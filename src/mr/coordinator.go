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
	// Try to find a task with a desired status and update it atomically
	acquireTask := func(taskType TaskType, statuses []TaskStatus) (Task, bool) {
		for idx := range c.tasks {
			t := &c.tasks[idx]
			if t.TaskType != taskType {
				continue
			}
			t.mu.Lock()
			for _, s := range statuses {
				if t.Status != s {
					continue
				}
				// Mark as in progress
				t.Status = StatusInProgress
				t.mu.Unlock()

				// Start timeout watcher
				go c.watchTaskTimeout(idx)
				return *t, true
			}
			t.mu.Unlock()
		}
		return Task{}, false
	}

	// searchListPreference := []struct {
	// 	taskType   TaskType
	// 	taskStatus []TaskStatus
	// }{
	// 	{
	// 		taskType:   TypeMap,
	// 		taskStatus: []TaskStatus{StatusPending},
	// 	},
	// 	{
	// 		taskType:   TypeMap,
	// 		taskStatus: []TaskStatus{StatusPending, StatusInProgress},
	// 	},
	// 	{
	// 		taskType:   TypeReduce,
	// 		taskStatus: []TaskStatus{StatusPending},
	// 	},
	// 	{
	// 		taskType:   TypeReduce,
	// 		taskStatus: []TaskStatus{StatusPending, StatusInProgress},
	// 	},
	// }

	// Prefer a "Pending" task first
	if task, ok := acquireTask(TypeMap, []TaskStatus{StatusPending}); ok {
		return task
	}

	// Fallback: also allow "InProgress" if nothing Pending
	if _, ok := acquireTask(TypeMap, []TaskStatus{StatusInProgress}); ok {
		return Task{TaskType: TypeWait}
	}

	// Prefer a "Pending" task first
	if task, ok := acquireTask(TypeReduce, []TaskStatus{StatusPending}); ok {
		return task
	}
	// Fallback: also allow "InProgress" if nothing Pending
	if task, ok := acquireTask(TypeReduce, []TaskStatus{StatusPending, StatusInProgress}); ok {
		return task
	}
	return Task{TaskType: TypeExit}
}

// Separate timeout watcher
func (c *Coordinator) watchTaskTimeout(idx int) {
	time.Sleep(10 * time.Second)
	t := &c.tasks[idx]

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Status != StatusFinished {
		t.Status = StatusPending
	}
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	reply.Task = c.getNextPendingTask()
	return nil
}

func (c *Coordinator) MarkFinished(args *MarkFinishedArgs, reply *MarkFinishedReply) error {
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
