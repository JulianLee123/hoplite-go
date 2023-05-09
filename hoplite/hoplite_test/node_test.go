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

func TestTaskTwoNodes(t *testing.T) {
	//test having object and objectId's shard stored on same node (n_a), 
	//and having n_b try to get this info. Second part tests same thing, but
	//objectId's shard stored on n_b node
	setup := MakeTestSetup(MakeBasicTwoNodes())
	shardNum := hoplite.GetShardForKey("ipt1",2)
	hostOdsNode := "n1" 
	otherOdsNode := "n2"
	if shardNum == 1{
		hostOdsNode = "n2" 
		otherOdsNode = "n1"
	}
	byteArr := hoplite.UInt64ToBytesArr([]uint64{5, 9, 10, 19})
	objMap := make(map[string][]byte)
	objMap["ipt1"] = byteArr
	err := setup.ScheduleTask(hostOdsNode, "opt", 1, []string{"ipt1"}, objMap)
	assert.Nil(t, err)
	byteAns, err := setup.GetTaskAns(otherOdsNode, "opt")
	assert.Nil(t, err)
	ans := hoplite.BytesToUInt64Arr(byteAns)
	assert.True(t, len(ans) == 2)
	//test having object stored on node n_a and objectId stored in shard in node n_b, 
	//and having n_b try to get this object*/
	shardNum = hoplite.GetShardForKey("ipt2",2)
	hostOdsNode = "n1" 
	otherOdsNode = "n2"
	if shardNum == 2{
		hostOdsNode = "n2" 
		otherOdsNode = "n1"
	}
	byteArr = hoplite.UInt64ToBytesArr([]uint64{5, 9, 10, 19})
	objMap = make(map[string][]byte)
	objMap["ipt2"] = byteArr
	err = setup.ScheduleTask(hostOdsNode, "opt2", 1, []string{"ipt2"}, objMap)
	assert.Nil(t, err)
	byteAns, err = setup.GetTaskAns(otherOdsNode, "opt2")
	assert.Nil(t, err)
	ans = hoplite.BytesToUInt64Arr(byteAns)
	assert.True(t, len(ans) == 2)
}

func TestTaskTwoNodesPromise(t *testing.T) {
	//test having object and objectId's shard stored on same node (n_a), 
	//and having n_b try to get this info. Second part tests same thing, but
	//objectId's shard stored on n_b node
	setup := MakeTestSetup(MakeBasicTwoNodes())
	shardNum := hoplite.GetShardForKey("tempipt1",2)
	hostOdsNode := "n1" 
	otherOdsNode := "n2"
	if shardNum == 1{
		hostOdsNode = "n2" 
		otherOdsNode = "n1"
	}
	byteArr := GenIptBytesArr(5000, 500,true)//use large numbers to create a task delay
	objMap := make(map[string][]byte)
	objMap["tempipt1"] = byteArr
	err := setup.ScheduleTask(hostOdsNode, "opt", 1, []string{"tempipt1"}, objMap)
	assert.Nil(t, err)
	//test promise on object "opt"
	byteArr = GenIptBytesArr(5000, 500,false)
	objMap = make(map[string][]byte)
	objMap["tempipt2"] = byteArr
	err = setup.ScheduleTask(otherOdsNode, "opt2", 2, []string{"tempipt2", "opt"}, objMap)
	assert.Nil(t, err)
	//retrieve result on original node
	byteAns, err := setup.GetTaskAns(hostOdsNode, "opt2")
	assert.Nil(t, err)
	ans := hoplite.BytesToUInt64Arr(byteAns)
	assert.True(t, len(ans) == 500)
	assert.True(t, ans[0] == uint64(5*4952019383323))
	time.Sleep(100 * time.Millisecond)
}