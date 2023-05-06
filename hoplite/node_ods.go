package hoplite

//Node ODS/object related tasks implementation

import (
	"context"
	"sync"
	"math/rand"

	"hoplite.go/hoplite/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


//ODS CALLS SENDING REQUESTS (CLIENT)

func (node *Node) Get(ctx context.Context, key string) (*proto.OdsInfo, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shardNumber := fnv32(key)%node.ods.shardMap.NumShards() + 1
	nodes := node.ods.shardMap.NodesForShard(shardNumber)

	if len(nodes) == 0 {
		return nil, false, status.Error(codes.InvalidArgument, "no nodes found")
	}

	randNode := rand.Intn(len(nodes)) //choose random node
	iterator := randNode
	start := true
	var response *proto.OdsGetResponse

	client, err := node.clientPool.GetClient(nodes[randNode])
	for {
		if (iterator%len(nodes)) == randNode && !start {
			return nil, false, err
		}
		start = false
		client, err = node.clientPool.GetClient(nodes[iterator%len(nodes)])
		if err != nil {
			iterator += 1
			continue
		} else {
			response, err = client.OdsGet(ctx, &proto.OdsGetRequest{Key: key})
			if err != nil {
				iterator += 1
				continue
			}
			break
		}
	}

	return response.Value, response.WasFound, nil
}

func concurrentSet(ctx context.Context, key string, value *proto.OdsInfo,
	errorChannel chan error, client proto.HopliteClient) {
	_, err := client.OdsSet(ctx, &proto.OdsSetRequest{Key: key, Value: value})
	errorChannel <- err
}

func (node *Node) Set(ctx context.Context, key string, value *proto.OdsInfo) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shardNumber := fnv32(key)%node.ods.shardMap.NumShards() + 1
	nodes := node.ods.shardMap.NodesForShard(shardNumber)

	if len(nodes) == 0 {
		return status.Error(codes.InvalidArgument, "no nodes found")
	}
	errorChannel := make(chan error)
	for _, node := range nodes {
		client, err := node.clientPool.GetClient(node)
		if err == nil {
			go concurrentSet(ctx, key, value, errorChannel, client)
		} else {
			go func(chnl chan error, er error) {
				chnl <- err
			}(errorChannel, err)
		}
	}
	var pError error = nil
	for i := 0; i < len(nodes); i++ {
		possibleError := <-errorChannel
		if possibleError != nil {
			pError = possibleError
		}
	}
	return pError
}

func concurrentDelete(ctx context.Context, key string, nodeName string,
	errorChannel chan error, client proto.HopliteClient) {
	_, err := client.Delete(ctx, &proto.OdsDeleteRequest{Key: key, NodeName: nodeName})
	errorChannel <- err
}

func (node *Node) Delete(ctx context.Context, key string, nodeName string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shardNumber := fnv32(key)%node.ods.shardMap.NumShards() + 1
	nodes := node.ods.shardMap.NodesForShard(shardNumber)

	if len(nodes) == 0 {
		return status.Error(codes.InvalidArgument, "no nodes found")
	}

	errorChannel := make(chan error)
	for _, node := range nodes {
		client, err := node.clientPool.GetClient(node)
		if err == nil {
			go concurrentDelete(ctx, key, nodeName, errorChannel, client)
		} else {
			go func(chnl chan error, er error) {
				chnl <- err
			}(errorChannel, err)
		}
	}
	var pError error = nil
	for i := 0; i < len(nodes); i++ {
		possibleError := <-errorChannel
		if possibleError != nil {
			pError = possibleError
		}
	}
	return pError
}

//ODS CALLS GENERATING RESPONSES (SERVER)

func (node *Node) OdsGetRes(
	ctx context.Context,
	request *proto.OdsGetRequest,
) (*proto.OdsGetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": node.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	key := request.Key
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key error")
	}

	//Find and check shard
	server.mu.RLock()
	defer server.mu.RUnlock()
	targetShard := GetShardForKey(key, server.shardMap.NumShards())
	_, shardExists := server.ourShards[targetShard]
	if !shardExists {
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	server.shard[targetShard].mu.RLock()
	defer server.shard[targetShard].mu.RUnlock()
	if server.shard[targetShard].data == nil {
		//config change --> no longer have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}

	val, exists := server.shard[targetShard].data[key]
	if !exists {
		return &proto.OdsGetResponse{Value: nil, WasFound: false}, nil
	}

	return &proto.OdsGetResponse{Value: val, WasFound: true}, nil
}

func (node *Node) OdsSetRes(
	ctx context.Context,
	request *proto.OdsSetRequest,
) (*proto.OdsSetResponse, error) {
	//only updates the part of the value associated with the provided node
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	key := request.Key
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key error")
	}

	//Find and check shard
	server.mu.RLock()
	defer server.mu.RUnlock()
	targetShard := GetShardForKey(key, server.shardMap.NumShards())
	_, shardExists := server.ourShards[targetShard]
	if !shardExists {
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	server.shard[targetShard].mu.Lock()
	defer server.shard[targetShard].mu.Unlock()
	if server.shard[targetShard].data == nil {
		//config change --> no longer have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}

	nodeToAdd := ""
	completionStatusToAdd := false
	for k := range request.Value.LocationInfos { //only holds 1 key
		nodeToAdd = k
		completionStatusToAdd = request.Value.LocationInfos[k]
	}
	server.shard[targetShard].data[key].LocationInfos[nodeToAdd] = completionStatusToAdd

	return &proto.OdsSetResponse{}, nil
}

func (node *Node) OdsDeleteRes(
	ctx context.Context,
	request *proto.OdsDeleteRequest,
) (*proto.OdsDeleteResponse, error) {
	//only deletes the part of the value associated with the provided node
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	key := request.Key
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key error")
	}

	//Find and check shard
	server.mu.RLock()
	defer server.mu.RUnlock()
	targetShard := GetShardForKey(key, server.shardMap.NumShards())
	_, shardExists := server.ourShards[targetShard]
	if !shardExists {
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	server.shard[targetShard].mu.Lock()
	defer server.shard[targetShard].mu.Unlock()
	if server.shard[targetShard].data == nil {
		//config change --> no loner have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}
	_, exists := server.shard[targetShard].data[key]
	if exists {
		_, exists = server.shard[targetShard].data[key].LocationInfos[request.NodeName]
		if exists {
			delete(server.shard[targetShard].data[key].LocationInfos, request.NodeName)
			if len(server.shard[targetShard].data[key].LocationInfos) == 0 {
				delete(server.shard[targetShard].data, key)
			}
		}
	}
	return &proto.OdsDeleteResponse{}, nil
}