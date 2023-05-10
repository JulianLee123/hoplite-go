package hoplitetest

import (
	//"fmt"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	/*"strings"
	"runtime"*/

	"github.com/stretchr/testify/assert"
	"hoplite.go/hoplite"
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
	byteAnsBc, err := setup.BroadcastObj("n1", "opt", 0)
	assert.Nil(t, err)
	ansBc := hoplite.BytesToUInt64Arr(byteAnsBc)
	assert.True(t, len(ansBc) == 2)

	//after deleting the object, it shouldn't broadcast
	err = setup.DeleteObj("n1", "opt")
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, err)
	_, err = setup.BroadcastObj("n1", "opt", 0)
	assert.NotNil(t, err)
}

func TestTaskTwoNodes(t *testing.T) {
	//test having object and objectId's shard stored on same node (n_a),
	//and having n_b try to get this info. Second part tests same thing, but
	//objectId's shard stored on n_b node
	setup := MakeTestSetup(MakeBasicTwoNodes())
	shardNum := hoplite.GetShardForKey("ipt1", 2)
	hostOdsNode := "n1"
	otherOdsNode := "n2"
	if shardNum == 1 {
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
	shardNum = hoplite.GetShardForKey("ipt2", 2)
	hostOdsNode = "n1"
	otherOdsNode = "n2"
	if shardNum == 2 {
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
	shardNum := hoplite.GetShardForKey("tempipt1", 2)
	hostOdsNode := "n1"
	otherOdsNode := "n2"
	if shardNum == 1 {
		hostOdsNode = "n2"
		otherOdsNode = "n1"
	}
	byteArr := GenIptBytesArr(5000, 500, true) //use large numbers to create a task delay
	objMap := make(map[string][]byte)
	objMap["tempipt1"] = byteArr
	err := setup.ScheduleTask(hostOdsNode, "opt", 1, []string{"tempipt1"}, objMap)
	assert.Nil(t, err)
	//test promise on object "opt"
	byteArr = GenIptBytesArr(5000, 505, false)
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

func LaunchConcurrentTasks(t *testing.T, setup *TestSetup, nodeNames []string, numTasks int, taskIsReduce bool) {
	targetTaskId := 2
	if taskIsReduce {
		targetTaskId = 3
	}
	//test processing 100 concurrent tasks and concurrent answer fetches, make sure answers are valid
	expObjLenMap := make(map[string]int) //expected lengths for produced objects
	for i := 0; i < numTasks; i++ {
		//schedule task asynchronous: don't need to launch separate goroutine
		objToMake := fmt.Sprintf("%s%d", "obj", i)
		nNum := rand.Int()%200 + 1
		nPrimes := rand.Int()%nNum + 1
		byteArr := GenIptBytesArr(nNum, nPrimes, false)
		objMap := make(map[string][]byte)
		objMap["tempipt1"] = byteArr
		if i < numTasks/2 {
			err := setup.ScheduleTask(nodeNames[rand.Int()%len(nodeNames)], objToMake, 1, []string{"tempipt1"}, objMap)
			assert.Nil(t, err)
			expObjLenMap[objToMake] = nPrimes
		} else {
			//for added complexity, schedule a series of tasks requiring promises on past tasks
			promisedObj := fmt.Sprintf("%s%d", "obj", i-numTasks/2)
			err := setup.ScheduleTask(nodeNames[rand.Int()%len(nodeNames)], objToMake, targetTaskId, []string{"tempipt1", promisedObj}, objMap)
			assert.Nil(t, err)
			if nNum < expObjLenMap[promisedObj] {
				expObjLenMap[objToMake] = nNum
			} else {
				expObjLenMap[objToMake] = expObjLenMap[promisedObj]
			}
		}
	}
	if taskIsReduce {
		return //not sure how to test answers for reduce
	}

	//get answers back
	var wg sync.WaitGroup
	for i := 0; i < numTasks; i++ {
		objToFind := fmt.Sprintf("%s%d", "obj", i)
		wg.Add(1)
		go func(i int) {
			byteAns, err := setup.GetTaskAns(nodeNames[rand.Int()%len(nodeNames)], objToFind)
			assert.Nil(t, err)
			ans := hoplite.BytesToUInt64Arr(byteAns)
			assert.True(t, len(ans) == expObjLenMap[objToFind])
			wg.Done()
		}(i)
	}
	wg.Wait()
	//delete all objects to save space
	for i := 0; i < numTasks; i++ {
		objToDel := fmt.Sprintf("%s%d", "obj", i)
		err := setup.DeleteGlobalObj(nodeNames[rand.Int()%len(nodeNames)], objToDel)
		assert.Nil(t, err)
	}
	time.Sleep(1000 * time.Millisecond) //wait for objects to be deleted
	//make sure objects no longer there: expect GetTaskAns to stall
	for i := 0; i < numTasks; i++ {
		objToFind := fmt.Sprintf("%s%d", "obj", i)
		go func(i int) {
			_, _ = setup.GetTaskAns(nodeNames[rand.Int()%len(nodeNames)], objToFind)
			assert.True(t, false)
		}(i)
	}
	time.Sleep(1000 * time.Millisecond) //make sure objects aren't there
}

func TestTaskOneNodeConcurrentTasks(t *testing.T) {
	numTasks := 100
	setup := MakeTestSetup(MakeBasicOneShard())
	LaunchConcurrentTasks(t, setup, []string{"n1"}, numTasks, false)
}

func TestTaskTwoNodesConcurrentTasks(t *testing.T) {
	numTasks := 100
	setup := MakeTestSetup(MakeBasicTwoNodes())
	LaunchConcurrentTasks(t, setup, []string{"n1", "n2"}, numTasks, false)
}

func TestTaskFiveNodesConcurrentTasks(t *testing.T) {
	numTasks := 100
	setup := MakeTestSetup(MakeFiveNodes())
	LaunchConcurrentTasks(t, setup, []string{"n1", "n2", "n3", "n4", "n5"}, numTasks, false)
}

func TestReduceBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicTwoNodes())
	shardNum := hoplite.GetShardForKey("tempipt1", 2)
	hostOdsNode := "n1"
	otherOdsNode := "n2"
	if shardNum == 1 {
		hostOdsNode = "n2"
		otherOdsNode = "n1"
	}
	byteArr := GenIptBytesArr(5000, 500, true) //use large numbers to create a task delay
	objMap := make(map[string][]byte)
	objMap["tempipt1"] = byteArr
	err := setup.ScheduleTask(hostOdsNode, "opt", 1, []string{"tempipt1"}, objMap)
	assert.Nil(t, err)
	//test promise on object "opt"
	byteArr = GenIptBytesArr(5000, 505, false)
	objMap = make(map[string][]byte)
	objMap["tempipt2"] = byteArr
	err = setup.ScheduleTask(otherOdsNode, "opt2", 3, []string{"tempipt2", "opt"}, objMap)
	assert.Nil(t, err)
	//retrieve result on original node
	byteAns, err := setup.GetTaskAns(hostOdsNode, "opt2")
	assert.Nil(t, err)
	ans := hoplite.BytesToUInt64Arr(byteAns)
	assert.True(t, len(ans) == 5000)

}

func TestReduceConcurrent(t *testing.T) {
	setup := MakeTestSetup(MakeFiveNodes())
	numReduce := 100
	LaunchConcurrentTasks(t, setup, []string{"n1", "n2", "n3", "n4", "n5"}, numReduce, true)
}
