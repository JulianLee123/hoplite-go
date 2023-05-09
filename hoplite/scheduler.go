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
	nodeBusy     []bool
	objIds       []string //list of used objIds
	clientPool   ClientPool
	shutdown     chan struct{}
	mu           sync.RWMutex
	numShards    int
	nodes        map[string]*Node
	ObjIdCounter int
}

func MakeTaskScheduler(clientPool ClientPool, doneCh chan struct{}, numShards int, nodes map[string]*Node) *TaskScheduler {

	busy := make([]bool, len(nodes))
	for i := 0; i < len(nodes); i++ {
		busy[i] = false
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

func (scheduler *TaskScheduler) ScheduleTask(taskId int32, args []string, objIdToObj map[string][]byte) int {
	scheduler.ObjIdCounter += 1
	go scheduler.ScheduleTaskHelper(taskId, args, objIdToObj, scheduler.ObjIdCounter)
	return scheduler.ObjIdCounter
}

func (scheduler *TaskScheduler) ScheduleTaskHelper(taskId int32, args []string, objIdToObj map[string][]byte, ObjIdCounter int) {

	for {
		var i int = 0
		for key := range scheduler.nodes {
			if scheduler.nodeBusy[i%len(scheduler.nodes)] {
				continue
			}
			client, err := scheduler.clientPool.GetClient(key)
			if err != nil {
				i += 1
				continue
			} else {
				ctx := context.Background()
				scheduler.nodeBusy[i] = true
				response, err := client.ScheduleTask(ctx, &proto.TaskRequest{ObjId: strconv.Itoa(ObjIdCounter), TaskId: taskId, Args: args, ObjIdToObj: objIdToObj})
				scheduler.nodeBusy[i] = false
				if err != nil {
					continue
				}
				if response == nil {
					continue
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (scheduler *TaskScheduler) RetrieveObject(objId string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var i int = 0

	for key := range scheduler.nodes {
		client, err := scheduler.clientPool.GetClient(key)
		if err != nil {
			continue
		}

		response, err := client.GetTaskAns(ctx, &proto.TaskAnsRequest{ObjId: objId})
		scheduler.nodeBusy[i] = false

		i += 1

		if err == nil {
			return response.Res, nil
		}
		// If there was an error continue to the next node
	}

	// If all nodes failed, return an error
	return nil, errors.New("all nodes failed")
}
