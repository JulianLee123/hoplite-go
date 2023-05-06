package kv

import (
	"context"
	"sync"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OdsShard struct {
	data map[string]*proto.OdsInfo //set to nil if shard not currently active
	mu   sync.RWMutex
}

type Ods struct {
	nodeName   string
	shardMap   *ShardMap
	ourShards  map[int]struct{}
	clientPool ClientPool
	shutdown   chan struct{}
	shard      []OdsShard
	mu         sync.RWMutex
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *Ods {
	server := Ods{
		nodeName:   nodeName,
		shardMap:   shardMap,
		ourShards:  make(map[int]struct{}),
		clientPool: clientPool,
		shard:      make([]OdsShard, shardMap.NumShards()+1),
	}
	//for _, i := range shardMap.ShardsForNode(nodeName){
	//server.shard[i].data = make(map[string]valueAndExpire)
	//server.ourShards[i] = struct{}{}
	//}
	return &server
}

func (server *Ods) Shutdown() {
	server.shutdown <- struct{}{}
}

func (server *Ods) OdsGetRes(
	ctx context.Context,
	request *proto.OdsGetRequest,
) (*proto.OdsGetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
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

func (server *Ods) OdsSetRes(
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
	for k := range request.Value.LocationInfos { //only nolds 1 key
		nodeToAdd = k
		completionStatusToAdd = request.Value.LocationInfos[k]
	}
	server.shard[targetShard].data[key].LocationInfos[nodeToAdd] = completionStatusToAdd

	return &proto.OdsSetResponse{}, nil
}

func (server *Ods) OdsDeleteRes(
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

func (server *Ods) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	targetShard := request.Shard
	server.shard[targetShard].mu.RLock()
	defer server.shard[targetShard].mu.RUnlock()
	if server.shard[targetShard].data == nil {
		//don't have this shard
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}
	shardContents := []*proto.GetShardValue{}
	for k, v := range server.shard[targetShard].data {
		kvEntry := &proto.GetShardValue{Key: k, Value: v}
		shardContents = append(shardContents, kvEntry)
	}
	return &proto.GetShardContentsResponse{Values: shardContents}, nil
}
