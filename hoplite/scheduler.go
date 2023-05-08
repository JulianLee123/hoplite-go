package hoplite

import (
	"context"
	"strconv"
	"sync"
	"time"

	"hoplite.go/hoplite/proto"
)

type TaskScheduler struct {
	nodeBusy   []bool
	objIds     []string //list of used objIds
	clientPool ClientPool
	shutdown   chan struct{}
	mu         sync.RWMutex
	numShards  int
	nodes      map[string]*Node
}

var objIdCounter int = 0

func MakeTaskScheduler(clientPool ClientPool, doneCh chan struct{}, numShards int, nodes map[string]*Node) *TaskScheduler {

	busy := make([]bool, len(nodes))
	for i := 0; i < len(nodes); i++ {
		busy[i] = false
	}
	scheduler := TaskScheduler{
		nodeBusy:   busy,
		objIds:     make([]string, 0),
		clientPool: clientPool,
		shutdown:   doneCh,
		numShards:  numShards,
		nodes:      nodes,
	}
	return &scheduler
}

func (scheduler *TaskScheduler) ScheduleTask(taskId int32, args []string, objIdToObj map[string][]byte) int {
	objIdCounter += 1
	go scheduler.ScheduleTaskHelper(taskId, args, objIdToObj, objIdCounter)
	return objIdCounter
}

func (scheduler *TaskScheduler) ScheduleTaskHelper(taskId int32, args []string, objIdToObj map[string][]byte, objIdCounter int) {

	for {
		var i int = 0
		for key, _ := range scheduler.nodes {
			if scheduler.nodeBusy[i] {
				continue
			}
			i += 1
			client, err := scheduler.clientPool.GetClient(key)
			if err != nil {
				continue
			} else {
				ctx := context.Background()
				response, err := client.ScheduleTask(ctx, &proto.TaskRequest{ObjId: strconv.Itoa(objIdCounter), TaskId: taskId, Args: args, ObjIdToObj: objIdToObj})
				if err != nil {
					continue
				}
				if response == nil {
					continue
				}
				scheduler.nodeBusy[i] = true
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (scheduler *TaskScheduler) RetrieveObject(objId string) ([]byte, error) {
	for {
		var i int = 0
		for key, _ := range scheduler.nodes {
			client, err := scheduler.clientPool.GetClient(key)
			if err != nil {
				i += 1
				continue
			} else {
				ctx := context.Background()
				response, err := client.GetTaskAns(ctx, &proto.TaskAnsRequest{ObjId: strconv.Itoa(objIdCounter)})
				scheduler.nodeBusy[i] = false
				return response.Res, err
			}
		}
	}

}