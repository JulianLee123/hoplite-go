package hoplitetest

import (
	//"fmt"
	"testing"
	"time"
	/*"math/rand"
	"strings"
	"sync"
	"runtime"*/

	"hoplite.go/hoplite"
	"github.com/stretchr/testify/assert"
)

func TestTaskBroadcastDeleteBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	
	byteArr := hoplite.UInt64ToBytesArr([]uint64{5, 9, 10, 19})
	objMap := make(map[string][]byte)
	objMap["ipt"] = byteArr
	err := setup.ScheduleTask("n1", "opt", 1, []string{"ipt"}, objMap)
	assert.Nil(t, err)
	byteAns, err := setup.GetTaskAns("n1", "opt")
	assert.Nil(t, err)
	ans := hoplite.BytesToUInt64Arr(byteAns)
	assert.True(t, len(ans) == 2)

	//try to extract same object using broadcast RPC
	byteAnsBc, err := setup.BroadcastObj("n1","opt",0)
	assert.Nil(t, err)
	ansBc := hoplite.BytesToUInt64Arr(byteAnsBc)
	assert.True(t, len(ansBc) == 2)

	//after deleting the object, it shouldn't broadcast
	err = setup.DeleteObj("n1","opt")
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, err)
	_, err = setup.BroadcastObj("n1","opt",0)
	assert.NotNil(t, err)
}

/*
Tests (2-node): 
test having object and objectId's shard stored on same node (n_a), and having n_b try to get this info
test having object stored on node n_a and objectId stored in shard in node n_b, and having n_b try to get this object
*/

/*
func TestTaskTwoNodes(t *testing.T) {
	//MAKE WORK: 

	setup := MakeTestSetup(MakeBasicTwoNodes())
	//test having object and objectId's shard stored on same node (n_a), 
	//and having n_b try to get this info
	shardNum := hoplite.GetShardForKey("ipt",2)
	hostOdsNode := "n1" 
	otherOdsNode := "n2"
	if shardNum == 2{
		hostOdsNode = "n2" 
		otherOdsNode = "n1"
	}
	byteArr := hoplite.UInt64ToBytesArr([]uint64{5, 9, 10, 19})
	objMap := make(map[string][]byte)
	objMap["ipt"] = byteArr
	err := setup.ScheduleTask("n1", "opt", 1, []string{"ipt"}, objMap)
	assert.Nil(t, err)
	byteAns, err := setup.GetTaskAns("n1", "opt")
	assert.Nil(t, err)
	ans := hoplite.BytesToUInt64Arr(byteAns)

	assert.True(t, len(ans) == 3)
	//test having object stored on node n_a and objectId stored in shard in node n_b, 
	//and having n_b try to get this object
}*/