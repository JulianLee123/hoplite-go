

type TaskScheduler struct {
	nodeBusy []bool          
	objIds []string //list of used objIds          
	clientPool ClientPool
	shutdown   chan struct{}
	mu         sync.RWMutex
}

var objIdCounter = 0; 

func MakeTaskScheduler(clientPool ClientPool, doneCh chan struct{}) *TaskScheduler {
	scheduler := TaskScheduler{
		nodeBusy = make([]bool), 
		objIds = make([]string), 
		clientPool = clientPool, 
		shutdown = doneCh, 
	}
	return &server; 
}


func (scheduler *TaskScheduler) ScheduleTask(taskId int32, args string[], objIdToObj map<string, []byte>) (string, err) {
	obIdCounter += 1; 
	go ScheduleTaskHelper(taskId, args, objIdToObj) 
	return objIdCounter; 
}

func (scheduler *TaskScheduler) sendAppendEntries(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (scheduler *TaskScheduler) ScheduleTaskHelper(taskId int32, args string[], objIdToObj map<string, []byte>, chan []byte) {
	nodes := kv.shardMap.NodesForShard(shardNumber)

	randNode := rand.Intn(len(nodes)) //choose random node
	iterator := randNode
	start := true
	var response *proto.OdsGetResponse

	client, err := kv.clientPool.GetClient(nodes[randNode])
	for { //TODO: HOW TO CHECK IF NODE BUSY? 
		if (iterator%len(nodes)) == randNode && !start {
			return nil, false, err
		}
		start = false
		client, err = kv.clientPool.GetClient(nodes[iterator%len(nodes)])
		if err != nil {
			iterator += 1
			continue
		} else {
			response, err = client.Get(ctx, &proto.OdsGetRequest{Key: key})
			if err != nil {
				iterator += 1
				continue
			}
			break
		}
	}
}


func (scheduler *TaskScheduler) RetrieveObject(objId string) ([]byte, err) {
	
}
