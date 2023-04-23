package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type valueAndExpire struct {
	value  *proto.OdsInfo
	expire int64 //time of expiration
}

type KvShard struct {
	data       map[string]valueAndExpire //set to nil if shard not currently active
	mu         sync.RWMutex
}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	ourShards  map[int]struct{}
	clientPool ClientPool
	shutdown   chan struct{}
	ttlShutdown chan struct{} //signals ttl goroutine to terminate
	shard      []KvShard
	mu         sync.RWMutex //manipulating shard info has separate write lock, only use write lock here for manipulating metadata, but use read lock for any read
	age int //update everytime we get a new shardmap
}

func (server *KvServerImpl) addShard(shardNum int, configChange bool, ch chan int, replicaNames []string, myName string) {
	//Update shard info
	success := false
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, replicaName := range(replicaNames){
		if replicaName == myName{
			continue
		}
		client, err := server.clientPool.GetClient(replicaName)
		if err == nil {
			shardInfo, err := client.GetShardContents(ctx, &proto.GetShardContentsRequest{Shard: int32(shardNum)})
			if err == nil {
				//copy the shard
				server.shard[shardNum].mu.Lock()
				server.shard[shardNum].data = make(map[string]valueAndExpire)
				for _, kvEntry := range shardInfo.Values{
					expire := time.Now().UnixMilli() + kvEntry.TtlMsRemaining
					server.shard[shardNum].data[kvEntry.Key] = valueAndExpire{value: kvEntry.Value, expire: expire}
				}
				server.shard[shardNum].mu.Unlock()
				success = true
			}
		}
	}
	if !success{
		if configChange{
			logrus.WithFields(
			logrus.Fields{"nodeName": myName, "shardNum": shardNum},
			).Error("node couldn't copy shard")
		}
		server.shard[shardNum].mu.Lock()
		server.shard[shardNum].data = make(map[string]valueAndExpire)
		server.shard[shardNum].mu.Unlock()
	}
	ch <- shardNum
}

func (server *KvServerImpl) handleShardMapUpdate(onInit bool) {
	server.mu.Lock()
	//defer server.mu.Unlock()
	newShards := make(map[int]struct{})
	for _, i := range server.shardMap.ShardsForNode(server.nodeName){
		newShards[i] = struct{}{}
	}
	//shard removals
	for i := range server.ourShards{
		_, ok := newShards[i]
		if !ok{
			//remove shard
			delete(server.ourShards, i)
			server.shard[i].mu.Lock()
			server.shard[i].data = nil
			server.shard[i].mu.Unlock()
		}
	}
	//shard additions
	numAdditions := 0
	doneCh := make(chan int)
	for i := range newShards{
		_, ok := server.ourShards[i]
		if !ok{
			//add shard
			/*logrus.WithFields(
			logrus.Fields{"nodeName": server.nodeName, "shardNum": i},
			).Debug("Updating config for for shard")*/
			numAdditions += 1
			replicaNames := server.shardMap.NodesForShard(i)
			myName := server.nodeName
			go server.addShard(i, onInit, doneCh, replicaNames, myName)
		}
	}

	//Update server metadata to reflect the addition of the new shard
	for i := 0; i < numAdditions; i++{
		shardNum := <-doneCh
		server.ourShards[shardNum] = struct{}{}
	}
	server.mu.Unlock()
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate(false)
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		ourShards:  make(map[int]struct{}),
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		ttlShutdown:   make(chan struct{}),
		shard:     make([]KvShard, shardMap.NumShards() + 1),
		age: 0,
	}
	//for _, i := range shardMap.ShardsForNode(nodeName){
		//server.shard[i].data = make(map[string]valueAndExpire)
		//server.ourShards[i] = struct{}{}
	//}
	go server.shardMapListenLoop()
	if usingTtl{
		go server.invalidateKVs()
	}
	server.handleShardMapUpdate(true)
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	if usingTtl{
		server.ttlShutdown <- struct{}{} //bottlenecks
	}
	server.listener.Close()
}

func (server *KvServerImpl) invalidateKVs() {
	//periodically runs ttl cleaning procedure
	startTime := time.Now().UnixMilli()  
	done := false
	for !done{
		select {
			case <-server.ttlShutdown:
				return
			default:
				if time.Now().UnixMilli() - startTime > 2000{
					done = true
				}
		}
	}
	//find invalidated data
	server.mu.RLock()
	defer server.mu.RUnlock()
	for shardNum := range server.ourShards {
		select {
			case <-server.ttlShutdown:
				return
			default:
		}
		toDelete := make(map[string]struct{})
		//Process shard
		server.shard[shardNum].mu.RLock()
		for k := range server.shard[shardNum].data{
			if server.shard[shardNum].data[k].expire < time.Now().UnixMilli(){
				toDelete[k] = struct{}{}
			}
		}
		server.shard[shardNum].mu.RUnlock()
		//evict invalidated data
		if len(toDelete) > 0{
			server.shard[shardNum].mu.Lock()
			for k := range toDelete{
				//check condition again to make sure nothing changed in the middle (e.g. it wasn't already deleted by delete)
				if server.shard[shardNum].data[k].expire < time.Now().UnixMilli(){
					delete(server.shard[shardNum].data, k)
				}
			}
			server.shard[shardNum].mu.Unlock()
		}
	}
	//spawn next invalidation goroutine
	go server.invalidateKVs()
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
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
	if !shardExists{
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	server.shard[targetShard].mu.RLock()
	defer server.shard[targetShard].mu.RUnlock()
	if server.shard[targetShard].data == nil{
		//config change --> no longer have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}

	val, exists := server.shard[targetShard].data[key]
	if !exists {
		return &proto.GetResponse{Value: nil, WasFound: false}, nil
	}
	if usingTtl{
		now := time.Now()
		timeLeft := val.expire - now.UnixMilli()
		if timeLeft < 0{
			//expired key
			return &proto.GetResponse{Value: nil, WasFound: false}, nil
		}
	}

	return &proto.GetResponse{Value: val.value, WasFound: true}, nil
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
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
	if !shardExists{
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	server.shard[targetShard].mu.Lock()
	defer server.shard[targetShard].mu.Unlock()
	if server.shard[targetShard].data == nil{
		//config change --> no longer have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}
	now := time.Now()
	_, exists := server.shard[targetShard].data[key]
	if !exists {
		server.shard[targetShard].data[key] = valueAndExpire{request.Value, now.UnixMilli() + request.TtlMs}
	} else{
		nodeToAdd := ""
		completionStatusToAdd := false
		for k := range request.Value.LocationInfos { //only nolds 1 key
			nodeToAdd = k
			completionStatusToAdd = request.Value.LocationInfos[k]
		}
		server.shard[targetShard].data[key].value.LocationInfos[nodeToAdd] = completionStatusToAdd
	}
	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
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
	if !shardExists{
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	server.shard[targetShard].mu.Lock()
	defer server.shard[targetShard].mu.Unlock()
	if server.shard[targetShard].data == nil{
		//config change --> no loner have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}
	_, exists := server.shard[targetShard].data[key]
	if exists {
		_, exists = server.shard[targetShard].data[key].value.LocationInfos[request.NodeName]
		if exists {
			delete(server.shard[targetShard].data[key].value.LocationInfos, request.NodeName)
			if len(server.shard[targetShard].data[key].value.LocationInfos) == 0 {
				delete(server.shard[targetShard].data, key)
			}
		}
   }
	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	targetShard := request.Shard
	server.shard[targetShard].mu.RLock()
	defer server.shard[targetShard].mu.RUnlock()
	if server.shard[targetShard].data == nil{
		//don't have this shard
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}
	shardContents := []*proto.GetShardValue{}
	for k, v := range server.shard[targetShard].data{
		kvEntry := &proto.GetShardValue{Key: k, Value: v.value, TtlMsRemaining: v.expire - time.Now().UnixMilli()}
		shardContents = append(shardContents, kvEntry)
	}
	return &proto.GetShardContentsResponse{Values: shardContents}, nil
}