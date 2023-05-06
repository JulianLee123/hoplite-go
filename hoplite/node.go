package hoplite

//General node and node worker method implementation

import (
	"context"
	"sync"

	"hoplite.go/hoplite/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OdsShard struct {
	data  map[string]*proto.OdsInfo //objId → objInfo; set to nil if shard not currently active
	mu   sync.RWMutex
}

type Ods struct {
	shardMap   *ShardMap
	ourShards  map[int]struct{}
	shard      []OdsShard
}

type LocalObj struct {
	data []byte
	mu    sync.RWMutex
}

type LocalObjStore struct {
	mp    map[string]LocalObj
	mu    sync.RWMutex
}

type Node struct {
	//GENERAL
	nodeName string
	shutdown   chan struct{}
	clientPool ClientPool //access to other nodes
	//OBJECT STORE
	localObjStore LocalObjStore
	//ODS SHARD MAP 
	ods Ods
}

func MakeNode(nodeName string, shardMap *ShardMap, clientPool ClientPool) *Node {
	localObjStore := LocalObjStore{
		mp:  make(map[string]LocalObj),
	}
	ods := Ods{
		ourShards:  make(map[int]struct{}),
		shard:      make([]OdsShard, shardMap.NumShards()+1),
	}
	node := Node{
		nodeName:   nodeName,
		shutdown:   make(chan struct{}),
		clientPool: clientPool,
		localObjStore: localObjStore,
		ods: ods,
	}
	return &node
}

func (node *Node) Shutdown() {
	node.shutdown <- struct{}{}
}

/*
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

