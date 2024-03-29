package hoplitetest

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/sirupsen/logrus"
	"hoplite.go/hoplite"
	"hoplite.go/hoplite/proto"
)

type TestSetup struct {
	shardMap   *hoplite.ShardMap
	nodes      map[string]*hoplite.Node
	clientPool TestClientPool
	ods        *hoplite.Ods
	ctx        context.Context
}

func containsString(arr []string, target string) bool {
	for _, a := range arr {
		if a == target {
			return true
		}
	}
	return false
}

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}

func GenIptBytesArr(n int, numPrime int, large bool) []byte {
	//if large set, uses large numbers to add latency to the task (takes longer to process)
	//note: doesn't matter we're using the same prime over and over; not necessary to use distinct primes/composites
	//to demonstrate our system works
	var intArr []uint64
	prime := uint64(5)
	if large{
		prime = uint64(4952019383323)
	}
	for i := 0; i < numPrime; i++{
		intArr = append(intArr, prime)
	}
	for j := numPrime; j < n; j++{
		intArr = append(intArr, prime + 1)
	}
	return hoplite.UInt64ToBytesArr(intArr)
}

func MakeTestSetup(shardMap hoplite.ShardMapState) *TestSetup {
	logrus.SetLevel(logrus.DebugLevel)
	setup := TestSetup{
		shardMap: &hoplite.ShardMap{},
		ctx:      context.Background(),
		nodes:    make(map[string]*hoplite.Node),
	}
	setup.shardMap.Update(&shardMap)
	for name := range setup.shardMap.Nodes() {
		setup.nodes[name] = hoplite.MakeNode(
			name,
			setup.shardMap,
			&setup.clientPool,
		)
	}
	setup.clientPool.Setup(setup.nodes)
	//setup.node = hoplite.MakeNode("main", setup.shardMap, &setup.clientPool)

	logrus.WithFields(
		logrus.Fields{"nodes": len(shardMap.Nodes), "shards": len(shardMap.ShardsToNodes)},
	).Debug("created test setup with servers")
	return &setup
}

func MakeBasicOneShard() hoplite.ShardMapState {
	return hoplite.ShardMapState{
		NumShards: 1,
		Nodes:     makeNodeInfos(1),
		ShardsToNodes: map[int][]string{
			1: {"n1"},
		},
	}
}

func MakeBasicMultiShard() hoplite.ShardMapState {
	return hoplite.ShardMapState{
		NumShards: 3,
		Nodes:     makeNodeInfos(1),
		ShardsToNodes: map[int][]string{
			1: {"n1"},
			2: {"n1"},
			3: {"n1"},
		},
	}
}

func MakeBasicTwoNodes() hoplite.ShardMapState {
	return hoplite.ShardMapState{
		NumShards: 2,
		Nodes:     makeNodeInfos(2),
		ShardsToNodes: map[int][]string{
			1: {"n1"},
			2: {"n2"},
		},
	}
}

func MakeBasicFiveNodes() hoplite.ShardMapState {
	return hoplite.ShardMapState{
		NumShards: 5,
		Nodes:     makeNodeInfos(5),
		ShardsToNodes: map[int][]string{
			1: {"n1"},
			2: {"n2"},
			3: {"n3"},
			4: {"n4"},
			5: {"n5"},
		},
	}
}

func MakeFiveNodes() hoplite.ShardMapState {
	return hoplite.ShardMapState{
		NumShards: 5,
		Nodes:     makeNodeInfos(5),
		ShardsToNodes: map[int][]string{
			1: {"n1","n2"},
			2: {"n2","n3"},
			3: {"n3","n4"},
			4: {"n4","n5"},
			5: {"n5","n1"},
		},
	}
}

func makeNodeInfos(n int) map[string]hoplite.NodeInfo {
	nodes := make(map[string]hoplite.NodeInfo)
	for i := 1; i <= n; i++ {
		nodes[fmt.Sprintf("n%d", i)] = hoplite.NodeInfo{Address: "", Port: int32(i)}
	}
	return nodes
}

func (ts *TestSetup) OdsGet(client string, key string) (*proto.OdsInfo, bool, error) {
	gotClient, _ := ts.clientPool.GetClient(client)
	if gotClient == nil {
		return nil, false, nil
	}
	res, err :=  gotClient.OdsGet(ts.ctx, &proto.OdsGetRequest{Key: key})
	if err != nil{
		return nil, false, err
	}
	return res.Value, res.WasFound, err
}

func (ts *TestSetup) OdsSet(node string, key string, val *proto.OdsInfo) (error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil
	}
	_, err := gotClient.OdsSet(ts.ctx, &proto.OdsSetRequest{Key: key, Value: val})
	return err
}

func (ts *TestSetup) OdsDelete(node string, key string, nodeEntryToDelete string) (error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil
	}
	_, err := gotClient.OdsDelete(ts.ctx, &proto.OdsDeleteRequest{Key: key, NodeName: nodeEntryToDelete})
	return err
}

func (ts *TestSetup) BroadcastObj(client string, objId string, start int) ([]byte, error) {
	gotClient, _ := ts.clientPool.GetClient(client)
	if gotClient == nil {
		return nil, nil
	}
	res, err :=  gotClient.BroadcastObj(ts.ctx, &proto.BroadcastObjRequest{ObjectId: objId, Start: int32(start)})
	if err != nil{
		return nil, err
	}
	return res.Object, err
}

func (ts *TestSetup) DeleteObj(node string, objId string) (error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil
	}
	_, err := gotClient.DeleteObj(ts.ctx, &proto.DeleteObjRequest{ObjectId: objId})
	return err
}

func (ts *TestSetup) ScheduleTask(node string, objId string, taskId int, args []string, objIdToObj map[string][]byte) (error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil
	}
	errChan := make(chan error)
	go func(errChan chan error) {
		_, err := gotClient.ScheduleTask(ts.ctx, &proto.TaskRequest{ObjId: objId, TaskId: int32(taskId), Args: args, ObjIdToObj: objIdToObj})
		errChan <- err
	}(errChan)
	err := <-errChan
	return err
}

func (ts *TestSetup) GetTaskAns(node string, objId string) ([]byte, error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil, nil
	}
	res, err :=  gotClient.GetTaskAns(ts.ctx, &proto.TaskAnsRequest{ObjId: objId})
	if err != nil{
		return nil, err
	}
	return res.Res, err
}

func (ts *TestSetup) DeleteGlobalObj(node string, objId string) (error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil
	}
	_, err := gotClient.DeleteGlobalObj(ts.ctx, &proto.DeleteGlobalObjRequest{ObjectId: objId})
	return err
}