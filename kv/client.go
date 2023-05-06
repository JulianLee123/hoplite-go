package kv

import (
	"context"
	"math/rand"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
