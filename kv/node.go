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

func MakeNode(nodeName string, shardMap *ShardMap, clientPool ClientPool) *Ods {
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

//methods associated w/ object management

func (server *Ods) GetGlobalObject(ctx context.Context, objId string, objIdToObj map[string][]byte) []byte {
	val, exists := objIdToObj[objId]

	if exists { //objectId in the pre-specified params
		return val
	}

	getRes, err := server.OdsGetRes(ctx, &proto.OdsGetRequest{Key: objId})

	if err != nil && getRes.Value != nil {
		return getRes.Value.LocationInfos[objId]
	}
}

// methods associated with worker
func RunTask(ctx context.Context, request *proto.TaskRequest) (*proto.TaskResponse, error) {
	//TODO: switch case
	return nil, nil
}

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool
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

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (*proto.OdsInfo, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shardNumber := fnv32(key)%kv.shardMap.NumShards() + 1
	nodes := kv.shardMap.NodesForShard(shardNumber)

	if len(nodes) == 0 {
		return nil, false, status.Error(codes.InvalidArgument, "no nodes found")
	}

	randNode := rand.Intn(len(nodes)) //choose random node
	iterator := randNode
	start := true
	var response *proto.OdsGetResponse

	client, err := kv.clientPool.GetClient(nodes[randNode])
	for {
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

	// response, err := client.Get(ctx, &proto.GetRequest{Key: key})
	// if err != nil {
	// 	return "", false, err
	// }

	return response.Value, response.WasFound, nil
}

func concurrentSet(ctx context.Context, key string, value *proto.OdsInfo, ttl time.Duration,
	errorChannel chan error, client proto.KvClient) {
	_, err := client.Set(ctx, &proto.OdsSetRequest{Key: key, Value: value})
	errorChannel <- err
}

func (kv *Kv) Set(ctx context.Context, key string, value *proto.OdsInfo, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shardNumber := fnv32(key)%kv.shardMap.NumShards() + 1
	nodes := kv.shardMap.NodesForShard(shardNumber)

	if len(nodes) == 0 {
		return status.Error(codes.InvalidArgument, "no nodes found")
	}
	errorChannel := make(chan error)
	for _, node := range nodes {
		client, err := kv.clientPool.GetClient(node)
		if err == nil {
			go concurrentSet(ctx, key, value, ttl, errorChannel, client)
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
	errorChannel chan error, client proto.KvClient) {
	_, err := client.Delete(ctx, &proto.OdsDeleteRequest{Key: key, NodeName: nodeName})
	errorChannel <- err
}

func (kv *Kv) Delete(ctx context.Context, key string, nodeName string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shardNumber := fnv32(key)%kv.shardMap.NumShards() + 1
	nodes := kv.shardMap.NodesForShard(shardNumber)

	if len(nodes) == 0 {
		return status.Error(codes.InvalidArgument, "no nodes found")
	}

	errorChannel := make(chan error)
	for _, node := range nodes {
		client, err := kv.clientPool.GetClient(node)
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
