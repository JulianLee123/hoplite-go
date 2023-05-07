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
	nodes      map[string]*hoplite.Ods
	clientPool TestClientPool
	node       *hoplite.Ods
	ctx        context.Context
}

func MakeTestSetup(shardMap hoplite.ShardMapState) *TestSetup {
	logrus.SetLevel(logrus.DebugLevel)
	setup := TestSetup{
		shardMap: &hoplite.ShardMap{},
		ctx:      context.Background(),
		nodes:    make(map[string]*hoplite.Ods),
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

func makeNodeInfos(n int) map[string]hoplite.NodeInfo {
	nodes := make(map[string]hoplite.NodeInfo)
	for i := 1; i <= n; i++ {
		nodes[fmt.Sprintf("n%d", i)] = hoplite.NodeInfo{Address: "", Port: int32(i)}
	}
	return nodes
}

func (ts *TestSetup) Get(key string) (*proto.OdsGetResponse, error) {
	return ts.node.OdsGetRes(ts.ctx, &proto.OdsGetRequest{Key: key})
}
