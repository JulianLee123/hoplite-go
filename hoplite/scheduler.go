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

type Response struct {
	res *proto.TaskResponse
	err error
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	for {
		var i int = 0
		for key := range scheduler.nodes {
			scheduler.mu.RLock()
			busyStatus := scheduler.nodeBusy[i]
			scheduler.mu.RUnlock()
			if busyStatus {
				i += 1
				continue
			}
			client, err := scheduler.clientPool.GetClient(key)
			if err != nil {
				i += 1
				continue
			} else {
				scheduler.mu.Lock()
				scheduler.nodeBusy[i] = true
				scheduler.mu.Unlock()

				currentChannel := make(chan Response, 1)
				go func() {
					response, _ := client.ScheduleTask(ctx, &proto.TaskRequest{ObjId: strconv.Itoa(ObjIdCounter), TaskId: taskId, Args: args, ObjIdToObj: objIdToObj})
					currentChannel <- Response{response, err}
				}()

				scheduler.mu.Lock()
				scheduler.nodeBusy[i] = false
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
		time.Sleep(10 * time.Millisecond)
	}
}

func (scheduler *TaskScheduler) RetrieveObject(objId string) ([]byte, error) {
	//synchronous: runs until object can be retrieved
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	var i int = 0
	for true {
		for key := range scheduler.nodes {
			scheduler.mu.RLock()
			if scheduler.nodeBusy[i] == true {
				scheduler.mu.RUnlock()
				continue
			}
			scheduler.mu.RUnlock()
			client, err := scheduler.clientPool.GetClient(key)
			if err != nil {
				i += 1
				continue
			}
			scheduler.mu.Lock()
			scheduler.nodeBusy[i] = true
			scheduler.mu.Unlock()
			response, err := client.GetTaskAns(ctx, &proto.TaskAnsRequest{ObjId: objId})
			scheduler.mu.Lock()
			scheduler.nodeBusy[i] = false
			scheduler.mu.Unlock()
			i += 1

			if err == nil {
				return response.Res, nil
			}
			// If there was an error continue to the next node
		}
	}

	// If all nodes failed, return an error
	return nil, errors.New("all nodes failed")
}
