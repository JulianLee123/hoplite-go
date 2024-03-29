package hoplitetest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"hoplite.go/hoplite"
	"hoplite.go/hoplite/proto"
)

type TestClientPool struct {
	mutex           sync.RWMutex
	getClientErrors map[string]error
	nodes           map[string]*TestClient
}

func (cp *TestClientPool) Setup(nodes map[string]*hoplite.Node) {
	cp.nodes = make(map[string]*TestClient)
	for nodeName, server := range nodes {
		cp.nodes[nodeName] = &TestClient{
			server: server,
			err:    nil,
		}
	}
}

func (cp *TestClientPool) GetClient(nodeName string) (proto.HopliteClient, error) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	err, ok := cp.getClientErrors[nodeName]
	if ok {
		return nil, err
	}

	if cp.nodes == nil {
		return nil, errors.Errorf("node %s node setup yet: test cluster may be starting", nodeName)
	}
	return cp.nodes[nodeName], nil
}

func (cp *TestClientPool) AddLatencyInjection(nodeName string, duration time.Duration) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].SetLatencyInjection(duration)
}
func (cp *TestClientPool) ClearRpcOverrides(nodeName string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].ClearOverrides()
}
func (cp *TestClientPool) GetRequestsSent(nodeName string) int {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	client := cp.nodes[nodeName]
	return int(atomic.LoadUint64(&client.requestsSent))
}
func (cp *TestClientPool) ClearRequestsSent(nodeName string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	client := cp.nodes[nodeName]
	atomic.StoreUint64(&client.requestsSent, 0)
}

func (cp *TestClientPool) ClearServerImpls() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	for nodeName := range cp.nodes {
		cp.nodes[nodeName].server = nil
	}
}

type TestClient struct {
	server *hoplite.Node
	// requestsSent is managed atomically so we don't need write locks per request
	requestsSent uint64

	// mutex protects below variables which act as mock responses
	// for testing
	mutex          sync.RWMutex
	err            error
	getResponse    *proto.OdsGetResponse
	setResponse    *proto.OdsSetResponse
	deleteResponse *proto.OdsDeleteResponse

	broadcastObjResponse *proto.BroadcastObjResponse
	deleteObjResponse    *proto.DeleteObjResponse

	taskResponse    *proto.TaskResponse
	taskAnsResponse *proto.TaskAnsResponse
	deleteGlobalObjResponse    *proto.DeleteGlobalObjResponse

	latencyInjection *time.Duration
}

func (c *TestClient) OdsGet(ctx context.Context, req *proto.OdsGetRequest, opts ...grpc.CallOption) (*proto.OdsGetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getResponse != nil {
		return c.getResponse, nil
	}
	return c.server.OdsGetRes(ctx, req)
}
func (c *TestClient) OdsSet(ctx context.Context, req *proto.OdsSetRequest, opts ...grpc.CallOption) (*proto.OdsSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.setResponse != nil {
		return c.setResponse, nil
	}
	return c.server.OdsSetRes(ctx, req)
}
func (c *TestClient) OdsDelete(ctx context.Context, req *proto.OdsDeleteRequest, opts ...grpc.CallOption) (*proto.OdsDeleteResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.deleteResponse != nil {
		return c.deleteResponse, nil
	}
	return c.server.OdsDeleteRes(ctx, req)
}

func (c *TestClient) ClearOverrides() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.err = nil
	c.getResponse = nil
	c.latencyInjection = nil
}

func (c *TestClient) OverrideError(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.err = err
}

/*
	 func (c *TestClient) OverrideGetResponse(val string, wasFound bool) {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.getResponse = &proto.OdsGetResponse{
			Value:    val,
			WasFound: wasFound,
		}
	}
*/
func (c *TestClient) OverrideSetResponse() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.setResponse = &proto.OdsSetResponse{}
}
func (c *TestClient) OverrideDeleteResponse() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.deleteResponse = &proto.OdsDeleteResponse{}
}

func (c *TestClient) SetLatencyInjection(duration time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.latencyInjection = &duration
}

func (c *TestClient) BroadcastObj(ctx context.Context, req *proto.BroadcastObjRequest, opts ...grpc.CallOption) (*proto.BroadcastObjResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.broadcastObjResponse != nil {
		return c.broadcastObjResponse, nil
	}
	return c.server.BroadcastLocalObject(ctx, req)
}

func (c *TestClient) DeleteObj(ctx context.Context, req *proto.DeleteObjRequest, opts ...grpc.CallOption) (*proto.DeleteObjResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.deleteObjResponse != nil {
		return c.deleteObjResponse, nil
	}
	return c.server.DeleteLocalObject(ctx, req)
}

func (c *TestClient) GetTaskAns(ctx context.Context, req *proto.TaskAnsRequest, opts ...grpc.CallOption) (*proto.TaskAnsResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	return c.server.RetrieveTaskAns(ctx, req)
}

func (c *TestClient) ScheduleTask(ctx context.Context, req *proto.TaskRequest, opts ...grpc.CallOption) (*proto.TaskResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.taskResponse != nil {
		return c.taskResponse, nil
	}
	return c.server.ScheduleTask(ctx, req)
}

func (c *TestClient) DeleteGlobalObj(ctx context.Context, req *proto.DeleteGlobalObjRequest, opts ...grpc.CallOption) (*proto.DeleteGlobalObjResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.deleteGlobalObjResponse != nil {
		return c.deleteGlobalObjResponse, nil
	}
	return c.server.DeleteGlobalObject(ctx, req)
}