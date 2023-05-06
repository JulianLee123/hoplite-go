package hoplite

type TaskScheduler struct {
	nodeBusy []bool          
	objIds []string //list of used objIds          
	clientPool ClientPool
	shutdown   chan struct{}
	numNods int
	mu         sync.RWMutex
}

var objIdCounter = 0; 

func MakeTaskScheduler(clientPool ClientPool, doneCh chan struct{}) *TaskScheduler {
	numNodes := len(clientPool.clients) 
	busy := make([]bool, numNodes)
	for i := range numNodes {
		busy[i] = false
	}
	scheduler := TaskScheduler{
		nodeBusy: busy, 
		objIds: make([]string), 
		clientPool: clientPool, 
		shutdown: doneCh, 
	}
	return &server; 
}

func (scheduler *TaskScheduler) ScheduleTask(taskId int32, args string[], objIdToObj map<string, []byte>) (string, err) {
	objIdCounter += 1
	go ScheduleTaskHelper(taskId, args, objIdToObj, objIdCounter) 
	return objIdCounter
}

func (scheduler *TaskScheduler) ScheduleTaskHelper(taskId int32, args string[], objIdToObj map<string, []byte>, chan []byte, objIdCounter int) {
	nodes := scheduler.shardMap.NodesForShard(shardNumber)

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
			response, err = client.Call(scheduler.RunTask(TaskRequest{obj_id=strconv.Itoa(objIdCounter), task_id=taskId, args=make([]string), obj_id_to_obj=objIdToObj}))
			if err != nil {
				iterator += 1
				continue
			}
			scheduler.nodeBusy[iterator % len(nodes)] = true
			break
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
			response, err = client.Call(scheduler.GetTaskAns(TaskAnsRequest{obj_id=strconv.Itoa(objId)}))
			if err != nil {
				iterator += 1
				continue
			}
			scheduler.nodeBusy[iterator % len(nodes)] = false
			break
		}
	}
}
