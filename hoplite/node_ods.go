package hoplite

//Node ODS/object related tasks implementation

import (
	"context"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"hoplite.go/hoplite/proto"
)

//OBJECT MANAGEMENT

// currently DOES NOT handle partial copies
func (node *Node) GetGlobalObject(ctx context.Context, objId string, objIdToObj map[string][]byte) []byte {
	//synchronous: runs until successful (can always be successful if 0-1 nodes down)

	//search pre-set
	obj, exists := objIdToObj[objId]
	if exists { //objectId in the pre-specified params
		return obj
	}

	for true {
		//search locally
		node.localObjStore.mu.RLock()
		objLocal, exists := node.localObjStore.mp[objId]
		if exists { //objectId in the pre-specified params
			objLocal.mu.RLock()
			if !objLocal.isPartial {
				defer objLocal.mu.RUnlock()
				defer node.localObjStore.mu.RUnlock()
				return objLocal.data
			}
			objLocal.mu.RUnlock()
		}
		node.localObjStore.mu.RUnlock()

		//search globally
		//find location in ODS
		odsInfo, wasFound, err := node.OdsGetReq(ctx, objId)
		if err != nil && wasFound && odsInfo != nil {
			for nodeWithInfo := range odsInfo.LocationInfos {
				if odsInfo.LocationInfos[nodeWithInfo] {
					//found node with complete copy: acquire it
					client, err := node.clientPool.GetClient(nodeWithInfo)
					if err != nil {
						response, err := client.BroadcastObj(ctx, &proto.BroadcastObjRequest{ObjectId: objId, Start: 0})
						if err != nil {
							//save object in local object store and return it
							node.localObjStore.mu.Lock()
							defer node.localObjStore.mu.Unlock()
							node.localObjStore.mp[objId] = LocalObj{data: response.Object, isPartial: false}
							return response.Object
						}
					}
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil //unreachable
}

func (node *Node) PutGlobalObject(ctx context.Context, objId string, obj []byte) {
	//Creates object with object Id in local object store, updates ODS
	//Synchronous: runs until successful (can always be successful if 0-1 nodes down)
	node.localObjStore.mu.Lock()
	defer node.localObjStore.mu.Unlock()
	node.localObjStore.mp[objId] = LocalObj{data: obj, isPartial: false}

	mp := make(map[string]bool)
	mp[node.nodeName] = true //storing a complete object
	odsInfo := &proto.OdsInfo{Size: int64(len(obj)), LocationInfos: mp}
	for true {
		err := node.OdsSetReq(ctx, objId, odsInfo)
		if err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (node *Node) DeleteGlobalObject(ctx context.Context, objId string) error {
	//Deletes object from all nodes: sends a delete object request to all necessary nodes
	//Error is set when we get a failure delete response from a node that
	//should be storing this object according to ODS
	odsInfo := &proto.OdsInfo{}
	for true {
		odsInfo, wasFound, err := node.OdsGetReq(ctx, objId)
		if err == nil {
			if !wasFound || odsInfo == nil {
				//nothing to delete
				return nil
			}
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	var retErr error = nil
	for nodeWithInfo := range odsInfo.LocationInfos {
		//send delete request to nodes with this object
		for true {
			client, err := node.clientPool.GetClient(nodeWithInfo)
			if err == nil {
				break
			}
			_, err = client.DeleteObj(ctx, &proto.DeleteObjRequest{ObjectId: objId}) //only send once: if it errors out, not t
			if err != nil {
				retErr = err
			}
		}
	}
	return retErr
}

func (node *Node) BroadcastLocalObject(ctx context.Context, request *proto.BroadcastObjRequest) (*proto.BroadcastObjResponse, error) {
	//Broadcasts full object (possible optimization: pipelining)
	node.localObjStore.mu.RLock()
	defer node.localObjStore.mu.RUnlock()
	objLocal, exists := node.localObjStore.mp[request.ObjectId]
	if !exists {
		return nil, status.Error(codes.NotFound, "node doesn't host object requested to be broadcast")
	}
	objLocal.mu.Lock()
	defer objLocal.mu.Unlock()
	if len(objLocal.data) <= int(request.Start) {
		//the requestor already has all the info on the object available on this node
		return nil, status.Error(codes.NotFound, "node doesn't have further info on this object (size(obj) < start)")
	}
	return &proto.BroadcastObjResponse{Object: objLocal.data[request.Start:]}, nil
}

func (node *Node) DeleteLocalObject(ctx context.Context, request *proto.DeleteObjRequest) (*proto.DeleteObjResponse, error) {
	//Deletes local copy of the object specified by the provided object Id
	node.localObjStore.mu.Lock()
	defer node.localObjStore.mu.Unlock()
	_, exists := node.localObjStore.mp[request.ObjectId]
	if !exists {
		return nil, status.Error(codes.NotFound, "node doesn't host object requested to be deleted")
	}
	delete(node.localObjStore.mp, request.ObjectId)
	return &proto.DeleteObjResponse{}, nil
}

//ODS CALLS SENDING REQUESTS (CLIENT)

func (node *Node) OdsGetReq(ctx context.Context, key string) (*proto.OdsInfo, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shardNumber := fnv32(key)%node.ods.shardMap.NumShards() + 1
	targetNodes := node.ods.shardMap.NodesForShard(shardNumber)

	if len(targetNodes) == 0 {
		return nil, false, status.Error(codes.InvalidArgument, "no nodes found")
	}

	randNode := rand.Intn(len(targetNodes)) //choose random node
	iterator := randNode
	start := true
	var response *proto.OdsGetResponse

	client, err := node.clientPool.GetClient(targetNodes[randNode])
	for {
		if (iterator%len(targetNodes)) == randNode && !start {
			return nil, false, err
		}
		start = false
		client, err = node.clientPool.GetClient(targetNodes[iterator%len(targetNodes)])
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

func (node *Node) OdsSetReq(ctx context.Context, key string, value *proto.OdsInfo) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shardNumber := fnv32(key)%node.ods.shardMap.NumShards() + 1
	targetNodes := node.ods.shardMap.NodesForShard(shardNumber)

	if len(targetNodes) == 0 {
		return status.Error(codes.InvalidArgument, "no nodes found")
	}
	errorChannel := make(chan error)
	for _, targetNode := range targetNodes {
		client, err := node.clientPool.GetClient(targetNode)
		if err == nil {
			go concurrentSet(ctx, key, value, errorChannel, client)
		} else {
			go func(chnl chan error, er error) {
				chnl <- err
			}(errorChannel, err)
		}
	}
	var pError error = nil
	for i := 0; i < len(targetNodes); i++ {
		possibleError := <-errorChannel
		if possibleError != nil {
			pError = possibleError
		}
	}
	return pError
}

func concurrentDelete(ctx context.Context, key string, nodeName string,
	errorChannel chan error, client proto.HopliteClient) {
	_, err := client.OdsDelete(ctx, &proto.OdsDeleteRequest{Key: key, NodeName: nodeName})
	errorChannel <- err
}

func (node *Node) OdsDeleteReq(ctx context.Context, key string, nodeName string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shardNumber := fnv32(key)%node.ods.shardMap.NumShards() + 1
	targetNodes := node.ods.shardMap.NodesForShard(shardNumber)

	if len(targetNodes) == 0 {
		return status.Error(codes.InvalidArgument, "no nodes found")
	}

	errorChannel := make(chan error)
	for _, targetNode := range targetNodes {
		client, err := node.clientPool.GetClient(targetNode)
		if err == nil {
			go concurrentDelete(ctx, key, nodeName, errorChannel, client)
		} else {
			go func(chnl chan error, er error) {
				chnl <- err
			}(errorChannel, err)
		}
	}
	var pError error = nil
	for i := 0; i < len(targetNodes); i++ {
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
	targetShard := GetShardForKey(key, node.ods.shardMap.NumShards())
	_, shardExists := node.ods.ourShards[targetShard]
	if !shardExists {
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this odsInfo")
	}

	//Process shard
	node.ods.shard[targetShard].mu.RLock()
	defer node.ods.shard[targetShard].mu.RUnlock()
	if node.ods.shard[targetShard].data == nil {
		//config change --> no longer have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this odsInfo")
	}

	val, exists := node.ods.shard[targetShard].data[key]
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
		logrus.Fields{"node": node.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	key := request.Key
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key error")
	}

	//Find and check shard
	targetShard := GetShardForKey(key, node.ods.shardMap.NumShards())
	_, shardExists := node.ods.ourShards[targetShard]
	if !shardExists {
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	node.ods.shard[targetShard].mu.Lock()
	defer node.ods.shard[targetShard].mu.Unlock()
	if node.ods.shard[targetShard].data == nil {
		//config change --> no longer have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}

	nodeToAdd := ""
	completionStatusToAdd := false
	for k := range request.Value.LocationInfos { //only holds 1 key
		nodeToAdd = k
		completionStatusToAdd = request.Value.LocationInfos[k]
	}
	node.ods.shard[targetShard].data[key].LocationInfos[nodeToAdd] = completionStatusToAdd

	return &proto.OdsSetResponse{}, nil
}

func (node *Node) OdsDeleteRes(
	ctx context.Context,
	request *proto.OdsDeleteRequest,
) (*proto.OdsDeleteResponse, error) {
	//only deletes the part of the value associated with the provided node
	logrus.WithFields(
		logrus.Fields{"node": node.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	key := request.Key
	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "empty key error")
	}

	//Find and check shard
	targetShard := GetShardForKey(key, node.ods.shardMap.NumShards())
	_, shardExists := node.ods.ourShards[targetShard]
	if !shardExists {
		return nil, status.Error(codes.NotFound, "server doesn't host shard for this kv")
	}

	//Process shard
	node.ods.shard[targetShard].mu.Lock()
	defer node.ods.shard[targetShard].mu.Unlock()
	if node.ods.shard[targetShard].data == nil {
		//config change --> no loner have this shard
		return nil, status.Error(codes.NotFound, "server no longer hosts shard for this kv")
	}
	_, exists := node.ods.shard[targetShard].data[key]
	if exists {
		_, exists = node.ods.shard[targetShard].data[key].LocationInfos[request.NodeName]
		if exists {
			delete(node.ods.shard[targetShard].data[key].LocationInfos, request.NodeName)
			if len(node.ods.shard[targetShard].data[key].LocationInfos) == 0 {
				delete(node.ods.shard[targetShard].data, key)
			}
		}
	}
	return &proto.OdsDeleteResponse{}, nil
}
