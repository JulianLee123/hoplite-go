package hoplitetest

import (
	"context"
	"fmt"

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

func MakeBasicTwoShard() hoplite.ShardMapState {
	return hoplite.ShardMapState{
		NumShards: 2,
		Nodes:     makeNodeInfos(2),
		ShardsToNodes: map[int][]string{
			1: {"n1"},
			2: {"n2"},
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

func (ts *TestSetup) Get(key string, client string) (*proto.OdsGetResponse, error) {
	gotClient, _ := ts.clientPool.GetClient(client)
	if gotClient == nil {
		return nil, nil
	}
	return gotClient.OdsGet(ts.ctx, &proto.OdsGetRequest{Key: key})
}

func (ts *TestSetup) Set(key string, node string, val *proto.OdsInfo) (*proto.OdsSetResponse, error) {
	gotClient, _ := ts.clientPool.GetClient(node)
	if gotClient == nil {
		return nil, nil
	}
	return gotClient.OdsSet(ts.ctx, &proto.OdsSetRequest{Key: key, Value: val})
}
