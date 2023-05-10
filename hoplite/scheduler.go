package hoplite

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"hoplite.go/hoplite/proto"
)

type TaskScheduler struct {
	nodeBusy     map[string]int //# of tasks running on that node
	objIds       []string //list of used objIds
	clientPool   ClientPool
	shutdown     chan struct{}
	mu           sync.RWMutex//lock only for nodeBusy
	numShards    int
	nodes        map[string]*Node
	ObjIdCounter int
}

type Response struct {
	res *proto.TaskResponse
	err error
}

func MakeTaskScheduler(clientPool ClientPool, doneCh chan struct{}, numShards int, nodes map[string]*Node) *TaskScheduler {

	busy := make(map[string]int, len(nodes))
   for key := range nodes {
      busy[key] = 0
   }
	scheduler := TaskScheduler{
		nodeBusy:     busy,
		objIds:       make([]string, 0),
		clientPool:   clientPool,
		shutdown:     doneCh,
		numShards:    numShards,
		nodes:        nodes,
		ObjIdCounter: 1,
	}
	return &scheduler
}

func (scheduler *TaskScheduler) findMostFreeNode() string {
   scheduler.mu.RLock()
   leastBusyKey := ""
   leastBusyNum := -1
   for key := range scheduler.nodes {
      if leastBusyNum == -1 || scheduler.nodeBusy[key] < leastBusyNum{
         leastBusyKey = key
			leastBusyNum = scheduler.nodeBusy[key]
      }
   }
	scheduler.mu.RUnlock()
   return leastBusyKey
}

func (scheduler *TaskScheduler) ScheduleTask(taskId int32, args []string, objIdToObj map[string][]byte) int {
	scheduler.ObjIdCounter += 1
	go scheduler.ScheduleTaskHelper(taskId, args, objIdToObj, scheduler.ObjIdCounter)
	return scheduler.ObjIdCounter
}

func (scheduler *TaskScheduler) ScheduleTaskHelper(taskId int32, args []string, objIdToObj map[string][]byte, ObjIdCounter int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	for {
		var i int = 0
		targetNodeKey := scheduler.findMostFreeNode()
		client, err := scheduler.clientPool.GetClient(targetNodeKey)
		if err != nil {
			i += 1
			continue
		} else {
			scheduler.mu.Lock()
			scheduler.nodeBusy[targetNodeKey] += 1
			scheduler.mu.Unlock()

			currentChannel := make(chan Response, 1)
			go func() {
				response, _ := client.ScheduleTask(ctx, &proto.TaskRequest{ObjId: strconv.Itoa(ObjIdCounter), TaskId: taskId, Args: args, ObjIdToObj: objIdToObj})
				currentChannel <- Response{response, err}
			}()

			scheduler.mu.Lock()
			scheduler.nodeBusy[targetNodeKey] -= 1
			scheduler.mu.Unlock()

			select {
			case res := <-currentChannel:
				if res.err != nil {
					i += 1
					continue
				}
				if res.res == nil {
					i += 1
					continue
				}
				return
			case <-time.After(1000 * time.Millisecond):
				i += 1
				continue
			}
		}
	}
}

func (scheduler *TaskScheduler) RetrieveObject(objId string) ([]byte, error) {
	//synchronous: runs until object can be retrieved
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	var i int = 0
	for true {
		targetNodeKey := scheduler.findMostFreeNode()
		client, err := scheduler.clientPool.GetClient(targetNodeKey)
		if err != nil {
			i += 1
			continue
		}
		scheduler.mu.Lock()
		scheduler.nodeBusy[targetNodeKey] += 1
		scheduler.mu.Unlock()
		response, err := client.GetTaskAns(ctx, &proto.TaskAnsRequest{ObjId: objId})
		scheduler.mu.Lock()
		scheduler.nodeBusy[targetNodeKey] += 1
		scheduler.mu.Unlock()
		i += 1

		if err == nil {
			return response.Res, nil
		}
	}

	//won't hit this b/c synchronous: guaranteed to be fulfilled
	// If all nodes failed, return an error
	return nil, errors.New("all nodes failed")
}
