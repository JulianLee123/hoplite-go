package hoplite

//General node and node worker method implementation

import (
	"context"
	"math"
	"sync"

	"hoplite.go/hoplite/proto"
)

type OdsShard struct {
	data map[string]*proto.OdsInfo //objId → objInfo; set to nil if shard not currently active
	mu   sync.RWMutex
}

type Ods struct {
	shardMap   *ShardMap
	ourShards  map[int]struct{}
	clientPool ClientPool
	shard      []OdsShard
}

type LocalObj struct {
	data      []byte
	isPartial bool
	mu        sync.RWMutex
}

type LocalObjStore struct {
	mp map[string]*LocalObj
	mu sync.RWMutex
}

type Node struct {
	//GENERAL
	nodeName   string
	shutdown   chan struct{}
	clientPool ClientPool //access to other nodes
	//OBJECT STORE
	localObjStore LocalObjStore
	//ODS SHARD MAP
	ods Ods
}

func MakeNode(nodeName string, shardMap *ShardMap, clientPool ClientPool) *Node {
	localObjStore := LocalObjStore{
		mp: make(map[string]*LocalObj),
	}
	ods := Ods{
		ourShards: make(map[int]struct{}),
		shard:     make([]OdsShard, shardMap.NumShards()+1),
		shardMap:  shardMap,
	}
	node := Node{
		nodeName:      nodeName,
		shutdown:      make(chan struct{}),
		clientPool:    clientPool,
		localObjStore: localObjStore,
		ods:           ods,
	}
	for _, i := range shardMap.ShardsForNode(nodeName) {
		node.ods.shard[i].data = make(map[string]*proto.OdsInfo)
		node.ods.ourShards[i] = struct{}{}
	}
	return &node
}

func (node *Node) Shutdown() {
	node.shutdown <- struct{}{}
}

//methods associated w/ object management
// methods associated with worker

func (node *Node) ScheduleTask(ctx context.Context, request *proto.TaskRequest) (*proto.TaskResponse, error) {
	if request.TaskId == 1 {
		node.SimulateCalcTask(ctx, request.Args[0], request.ObjId, request.ObjIdToObj)
	} else if request.TaskId == 2 {
		node.SimulateCalcWithPromiseTask(ctx, request.Args[0], request.Args[1], request.ObjId, request.ObjIdToObj)
	} else if request.TaskId == 3 {
		node.ReduceBasicTask(ctx, request.Args, request.ObjId, request.ObjIdToObj)
	}
	return &proto.TaskResponse{}, nil
}

func (node *Node) SimulateCalcTask(ctx context.Context, objId string, retObjId string, objIdToObj map[string][]byte) {
	//Sample task that extracts the prime numbers from the provided array of type uint64
	var intArr []uint64
	objBuff := node.GetGlobalObject(ctx, objId, objIdToObj, nil)
	intArr = BytesToUInt64Arr(objBuff)

	optArr := make([]uint64, 0)
	for _, val := range intArr {
		check := true
		for j := uint64(2); j < uint64(math.Sqrt(float64(val)))+1; j++ {
			if val%j == 0 {
				check = false
				break
			}
		}
		if check {
			optArr = append(optArr, val)
		}
	}
	optByteArr := UInt64ToBytesArr(optArr)
	node.PutGlobalObject(ctx, retObjId, optByteArr)
	return
}

func (node *Node) SimulateCalcWithPromiseTask(ctx context.Context, objId string, objId2 string, retObjId string, objIdToObj map[string][]byte) {
	//Sample task testing promises: pairwise multiplies elements in obj1 and obj2
	objBuff1 := node.GetGlobalObject(ctx, objId, objIdToObj, nil)
	objBuff2 := node.GetGlobalObject(ctx, objId2, objIdToObj, nil)
	primesArr1 := BytesToUInt64Arr(objBuff1)
	primesArr2 := BytesToUInt64Arr(objBuff2)
	optArr := make([]uint64, 0)
	for i := 0; i < len(primesArr1) && i < len(primesArr2); i++ {
		optArr = append(optArr, primesArr1[i]*primesArr2[i])
	}
	optByteArr := UInt64ToBytesArr(optArr)
	node.PutGlobalObject(ctx, retObjId, optByteArr)
	return
}

func (node *Node) ReduceBasicTask(ctx context.Context, objIds []string, retObjId string, objIdToObj map[string][]byte) {
	//Reduces the provided objects: A[i] in the output is the product of the ith entries of the provided objects (for all
	//objects for which the ith entry exists)

	//Channel will be set to true once object is available (if it's on another node, then once it's broadcasted)
	chans := make(map[int]chan struct{})
	for i := 1; i <= len(objIds); i++ {
		chans[i] = make(chan struct{})
		go node.GetGlobalObject(ctx, objIds[i-1], objIdToObj, chans[i])
	}

	optArr := make([]uint64, 0)
	reductions := 0
	for reductions = 0; reductions < len(objIds); {
		for i := 1; i <= len(chans); i++ {
			select {
			case <-chans[i]:
				//This node has access to the requested object --> start download
				objBuff := node.GetGlobalObject(ctx, objIds[i-1], objIdToObj, nil)
				intArr := BytesToUInt64Arr(objBuff)
				j := 0
				for ; j < len(optArr) && j < len(intArr); j++ {
					optArr[j] = optArr[j] * intArr[j]
				}
				for ; j < len(intArr); j++ {
					optArr = append(optArr, intArr[j])
				}
				reductions += 1
			default:
				continue
			}
		}
	}
	optByteArr := UInt64ToBytesArr(optArr)
	node.PutGlobalObject(ctx, retObjId, optByteArr)
	return
}

func (node *Node) RetrieveTaskAns(ctx context.Context, request *proto.TaskAnsRequest) (*proto.TaskAnsResponse, error) {
	//runs until outcome available: up to taskScheduler to make sure object eventually appears in case of node failure
	return &proto.TaskAnsResponse{Res: node.GetGlobalObject(ctx, request.ObjId, nil, nil)}, nil
}

/*
	fnv32 hash function taken from concurrent map implementation linked in assignment description

(https://github.com/orcaman/concurrent-map/blob/master/concurrent_map.go#L345-L354)
*/
func fnv32(key string) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return int(hash)
}
