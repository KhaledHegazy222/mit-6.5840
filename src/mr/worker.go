package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
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

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Your worker implementation here.

	for {
		t, err := FetchTask()
		if err != nil {
			log.Fatalf("Unexpected Err: %s", err)
		}
		if t.TaskType == TypeExit {
			return
		}
		if t.TaskType == TypeWait {
			continue
		}

		randomStr := RandStringRunes(10)
		filesWriter := make([]*os.File, 0, len(t.Output))
		for _, outputFile := range t.Output {
			f, err := os.OpenFile("temp-"+randomStr+outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Fatal(err)
			}
			filesWriter = append(filesWriter, f)
		}
		switch t.TaskType {
		case TypeMap:
			inputFile := t.Input[0]
			content, err := os.ReadFile(inputFile)
			if err != nil {
				log.Fatalf("can't open such file (%s), err: %s", inputFile, err)
			}
			mapResult := mapf(inputFile, string(content))
			for _, kvPair := range mapResult {
				bucket := ihash(kvPair.Key) % len(t.Output)
				fmt.Fprintf(filesWriter[bucket], "%v %v\n", kvPair.Key, kvPair.Value)
			}

		case TypeReduce:
			content := []KeyValue{}
			for _, inputFile := range t.Input {
				f, err := os.OpenFile(inputFile, os.O_RDONLY, 0644)
				if err != nil {
					log.Fatal(err)
				}
				for {
					kv := KeyValue{}
					_, err := fmt.Fscanf(f, "%v %v", &kv.Key, &kv.Value)
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Fatal(err)
					}
					content = append(content, kv)
				}
				f.Close()
			}
			sort.Sort(ByKey(content))
			i := 0
			for i < len(content) {
				j := i + 1
				for j < len(content) && content[j].Key == content[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, content[k].Value)
				}
				output := reducef(content[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(filesWriter[0], "%v %v\n", content[i].Key, output)
				i = j
			}
		}
		for idx := range filesWriter {
			f := filesWriter[idx]
			outputFile := t.Output[idx]
			err := f.Close()
			if err != nil {
				log.Fatalf("Unexpected Error: %v", err)
			}
			err = os.Rename("temp-"+randomStr+outputFile, outputFile)
			if err != nil {
				log.Fatalf("Unexpected Error: %v", err)
			}
		}
		MarkFinshed(t.Idx)
	}
}

func FetchTask() (Task, error) {
	// declare an argument structure.
	args := FetchTaskArgs{}

	// // fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := FetchTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.FetchTask", &args, &reply)
	if ok {
		return reply.Task, nil
	} else {
		return Task{}, fmt.Errorf("RPC Call Failed")
	}
}

func MarkFinshed(idx int) error {
	// declare an argument structure.
	args := MarkFinishedArgs{
		Idx: idx,
	}

	// // fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := MarkFinishedReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("RPC Call Failed")
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
