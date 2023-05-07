package hoplite

import (
	"math/rand"
	"strconv"
	"sync"

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
		for _, val := range scheduler.nodes {
			if scheduler.nodeBusy[i] {
				continue
			}
			i += 1
			client, err := scheduler.clientPool.GetClient(val)
			if err != nil {
				continue
			} else {
				response, err = client.Call(scheduler.RunTask(proto.TaskRequest{ObjId: strconv.Itoa(objIdCounter), TaskId: taskId, Args: args, ObjIdToObj: objIdToObj}))
				if err != nil {
					continue
				}
				scheduler.nodeBusy[i] = true
				break
			}
		}
	}

}

func (scheduler *TaskScheduler) RetrieveObject(objId string) ([]byte, err) {
	randNode := rand.Intn(len(nodes)) //choose random node
	iterator := randNode
	start := true
	var response *proto.OdsGetResponse

	client, err := scheduler.clientPool.GetClient(nodes[randNode])
	for { //TODO: HOW TO CHECK IF NODE BUSY?
		if (iterator%len(nodes)) == randNode && !start {
			return nil, false, err
		}
		start = false
		client, err = scheduler.clientPool.GetClient(nodes[iterator%len(nodes)])
		if err != nil {
			iterator += 1
			continue
		} else {
			response, err = client.Call(scheduler.GetTaskAns(TaskAnsRequest{obj_id: strconv.Itoa(objId)}))
			if err != nil {
				iterator += 1
				continue
			}
			scheduler.nodeBusy[iterator%len(nodes)] = false
			break
		}
	}
}
