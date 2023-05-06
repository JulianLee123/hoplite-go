package kvtest

import (
	"errors"
	"strconv"
	"testing"
	"context"
	"time"

	"github.com/stretchr/testify/assert"
	"cs426.yale.edu/lab4/kv/proto"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

// func TestYourTest1(t *testing.T) {
// 	assert.True(t, true)
// }

func TestClientGetError(t *testing.T) {
	setup := MakeTestSetupWithoutServers(MakeTwoNodeBothAssignedSingleShard())
	setup.clientPool.OverrideGetResponse("n1", "val", true)
	setup.clientPool.OverrideGetResponse("n2", "val", true)

	setup.clientPool.AddLatencyInjection("n1", 300*time.Millisecond)
	setup.clientPool.OverrideGetClientError("n1", errors.New("injected"))

	_, _, err := setup.Get("val")
	assert.Equal(t, 1, setup.clientPool.GetRequestsSent("n2"))
	assert.Nil(t, err)

	setup.clientPool.OverrideGetClientError("n2", errors.New("injected"))
	_, _, err = setup.Get("val")
	assert.NotNil(t, err)
}

func TestClientMultiSetDelete(t *testing.T) {
	setup := MakeTestSetupWithoutServers(MakeTwoNodeBothAssignedSingleShard())
	setup.clientPool.OverrideSetResponse("n1")
	setup.clientPool.OverrideSetResponse("n2")
	setup.clientPool.OverrideDeleteResponse("n1")
	setup.clientPool.OverrideDeleteResponse("n2")
	setup.clientPool.AddLatencyInjection("n1", 100*time.Millisecond)
	var numTimes int = 5
	for i := 0; i < numTimes; i++ {
		err := setup.Set("key", "val", 500*time.Millisecond)
		assert.Nil(t, err)
	}
	assert.Equal(t, numTimes, setup.clientPool.GetRequestsSent("n1"))
	assert.Equal(t, numTimes, setup.clientPool.GetRequestsSent("n2"))

	setup.Delete("key")

	assert.Equal(t, numTimes+1, setup.clientPool.GetRequestsSent("n1"))
	assert.Equal(t, numTimes+1, setup.clientPool.GetRequestsSent("n2"))
}

func TestServerTTL(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	var numTimes int = 5
	for i := 0; i < numTimes; i++ {
		setup.NodeSet("n1", "key"+strconv.Itoa(i), "val"+strconv.Itoa(i), 10*time.Millisecond)
	}
	setup.NodeSet("n1", "key"+strconv.Itoa(numTimes), "val"+strconv.Itoa(numTimes), 200*time.Millisecond)

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < numTimes-1; i++ {
		_, wasFound, err := setup.NodeGet("n1", "key"+strconv.Itoa(i))
		assert.Nil(t, err)
		assert.False(t, wasFound)
	}
	_, wasFound, err := setup.NodeGet("n1", "key"+strconv.Itoa(numTimes))
	assert.Nil(t, err)
	assert.True(t, wasFound)
	setup.Shutdown()
}

func TestServerMultiServerShardDropDataDeleted(t *testing.T) {
	// If a shard is dropped and added back to a different server S2,
	//the data should be deleted in between, and S2 should not error out.
	// Starts with n1: {1,2,3,4,5} and n2: {6,7,8,9,10}
	setup := MakeTestSetup(MakeTwoNodeMultiShard())
	err1 := setup.NodeSet("n1", "abc", "123", 10*time.Second)
	err2 := setup.NodeSet("n2", "abc", "123", 10*time.Second)
	assert.True(t, err1 == nil || err2 == nil)
	usedNode := ""
	unusedNode := ""
	if err1 == nil{
		usedNode = "n1"
		unusedNode = "n2"
		setup.UpdateShardMapping(map[int][]string{
			1:  {},
			2:  {},
			3:  {},
			4:  {},
			5:  {},
			6:  {"n2"},
			7:  {"n2"},
			8:  {"n2"},
			9:  {"n2"},
			10: {"n2"},
		})
	} else{
		usedNode = "n2"
		unusedNode = "n1"
		setup.UpdateShardMapping(map[int][]string{
			1:  {"n1"},
			2:  {"n1"},
			3:  {"n1"},
			4:  {"n1"},
			5:  {"n1"},
			6:  {},
			7:  {},
			8:  {},
			9:  {},
			10: {},
		})
	}

	_, _, err := setup.NodeGet(usedNode, "abc")
	assertShardNotAssigned(t, err)

	// Add used shard to the unused server
	setup.UpdateShardMapping(map[int][]string{
		1:  {unusedNode},
		2:  {unusedNode},
		3:  {unusedNode},
		4:  {unusedNode},
		5:  {unusedNode},
		6:  {unusedNode},
		7:  {unusedNode},
		8:  {unusedNode},
		9:  {unusedNode},
		10: {unusedNode},
	})

	_, wasFound, err := setup.NodeGet(unusedNode, "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	setup.Shutdown()
}

func TestServerGetShardContentsBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	err := setup.NodeSet("n1", "abc", "123", 10*time.Second)
	assert.Nil(t, err)
	res, err := setup.nodes["n1"].GetShardContents(
		context.Background(),
		&proto.GetShardContentsRequest{Shard: 1},
	)
	assert.Nil(t, err)
	assert.True(t, res.Values[0].Value == "123")
	assert.True(t, res.Values[0].Key == "abc")
	assert.True(t, res.Values[0].TtlMsRemaining <= 1000*10)
	setup.Shutdown()
}