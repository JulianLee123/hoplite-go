package hoplite

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"hoplite.go/hoplite/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func makeConnection(addr string) (proto.HopliteClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	channel, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return proto.NewHopliteClient(channel), nil
}

/*
 * Clients are cached by nodeName. We assume that the connection information (Address/Port)
 * will never change for a given nodeName.
 *
 * Use ClientPool::GetClient()
 */
type ClientPool interface {
	/*
	 * Returns a client for a given node if one can be created. Returns (nil, err)
	 * otherwise. Errors are not cached, so subsequent calls may return a valid client.
	 */
	GetClient(nodeName string) (proto.HopliteClient, error)
}

type GrpcClientPool struct {
	shardMap *ShardMap

	mutex   sync.RWMutex
	clients map[string]proto.HopliteClient
}

func MakeClientPool(shardMap *ShardMap) GrpcClientPool {
	return GrpcClientPool{shardMap: shardMap, clients: make(map[string]proto.HopliteClient)}
}

func (pool *GrpcClientPool) GetClient(nodeName string) (proto.HopliteClient, error) {
	// Optimistic read -- most cases we will have already cached the client, so
	// only take a read lock to maximize concurrency here
	pool.mutex.RLock()
	client, ok := pool.clients[nodeName]
	pool.mutex.RUnlock()
	if ok {
		return client, nil
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	// We may have lost a race and someone already created a client, try again
	// while holding the exclusive lock
	client, ok = pool.clients[nodeName]
	if ok {
		return client, nil
	}

	nodeInfo, ok := pool.shardMap.Nodes()[nodeName]
	if !ok {
		logrus.WithField("node", nodeName).Errorf("unknown nodename passed to GetClient")
		return nil, fmt.Errorf("no node named: %s", nodeName)
	}

	// Otherwise create the client: gRPC expects an address of the form "ip:port"
	address := fmt.Sprintf("%s:%d", nodeInfo.Address, nodeInfo.Port)
	client, err := makeConnection(address)
	if err != nil {
		logrus.WithField("node", nodeName).Debugf("failed to connect to node %s (%s): %q", nodeName, address, err)
		return nil, err
	}
	pool.clients[nodeName] = client
	return client, nil
}
