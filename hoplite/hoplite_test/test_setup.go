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