package hoplitetest

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"hoplite.go/hoplite"
)

func TestBasicScheduleOneNode(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	doneCh := make(chan struct{})
	scheduler := hoplite.MakeTaskScheduler(&setup.clientPool, doneCh, setup.shardMap.NumShards(),
		setup.nodes)

	//array of integers transformed into byte array
	byteArray := hoplite.UInt64ToBytesArr([]uint64{2, 5, 8, 3, 15})

	//add that node n1 will hold the object "0"
	// odsInfo := hoplite.CreateUpdateOdsInfo(10, "n1", true, nil)
	// err := setup.OdsSet("n1", "0", odsInfo)
	// assert.Nil(t, err)

	//schedule task of id 1 to filter primes out of the array
	objectMap := make(map[string][]byte)
	objectMap["0"] = byteArray
	result := scheduler.ScheduleTask(1, []string{"0"}, objectMap)

	time.Sleep(100 * time.Millisecond)

	val, err := scheduler.RetrieveObject(strconv.Itoa(result))

	assert.Nil(t, err)

	primes := hoplite.BytesToUInt64Arr(val)
	assert.Equal(t, len(primes), 3)
}

func TestBasicScheduleOneNodeMultiShard(t *testing.T) {
	setup := MakeTestSetup(MakeBasicMultiShard())

	doneCh := make(chan struct{})
	scheduler := hoplite.MakeTaskScheduler(&setup.clientPool, doneCh, setup.shardMap.NumShards(),
		setup.nodes)

	//array of integers transformed into byte array
	byteArray := hoplite.UInt64ToBytesArr([]uint64{2, 5, 8, 3, 15})

	//add that node n1 will hold the object "0"
	// odsInfo := hoplite.CreateUpdateOdsInfo(10, "n1", true, nil)
	// err := setup.OdsSet("n1", "0", odsInfo)
	// assert.Nil(t, err)

	//add the byteArray as a global object

	//schedule task of id 1 to filter primes out of the array
	objectMap := make(map[string][]byte)
	objectMap["0"] = byteArray
	result := scheduler.ScheduleTask(1, []string{"0"}, objectMap)

	time.Sleep(100 * time.Millisecond)

	val, err := scheduler.RetrieveObject(strconv.Itoa(result))

	assert.Nil(t, err)

	primes := hoplite.BytesToUInt64Arr(val)
	assert.Equal(t, len(primes), 3)
}

func TestBasicScheduleTwoNodes(t *testing.T) {
	setup := MakeTestSetup(MakeBasicTwoNodes())

	doneCh := make(chan struct{})
	scheduler := hoplite.MakeTaskScheduler(&setup.clientPool, doneCh, setup.shardMap.NumShards(),
		setup.nodes)

	//array of integers transformed into byte array
	byteArray1 := hoplite.UInt64ToBytesArr([]uint64{2, 5, 8, 3, 15})
	byteArray2 := hoplite.UInt64ToBytesArr([]uint64{2, 5, 8, 3, 15})

	//add the byteArray as a global object

	//schedule task of id 1 to filter primes out of the array
	objectMap := make(map[string][]byte)
	objectMap["first"] = byteArray1
	objectMap["second"] = byteArray2
	result1 := scheduler.ScheduleTask(1, []string{"first"}, objectMap)
	result2 := scheduler.ScheduleTask(1, []string{"second"}, objectMap)

	result := scheduler.ScheduleTask(2, []string{strconv.Itoa(result2), strconv.Itoa(result1)}, objectMap)

	time.Sleep(100 * time.Millisecond)

	val, err := scheduler.RetrieveObject(strconv.Itoa(result))

	assert.Nil(t, err)

	primes := hoplite.BytesToUInt64Arr(val)
	assert.Equal(t, len(primes), 3)
}

func LaunchConcurrentTasksScheduler(t *testing.T, setup *TestSetup, numTasks int) {
	doneCh := make(chan struct{})
	scheduler := hoplite.MakeTaskScheduler(&setup.clientPool, doneCh, setup.shardMap.NumShards(),
		setup.nodes)
	//test processing 100 concurrent tasks and concurrent answer fetches, make sure answers are valid
	expObjLenMap := make(map[string]int) //expected lengths for produced objects

	outputObjects := make([]int, 0)
	count := 0
	for i := 0; i < numTasks; i++ {
		//schedule task asynchronous: don't need to launch separate goroutine
		nNum := rand.Int()%200 + 1
		nPrimes := rand.Int()%nNum + 1
		byteArr := GenIptBytesArr(nNum, nPrimes, false)
		objMap := make(map[string][]byte)
		objMap["tempipt1"] = byteArr
		var toMake int
		if scheduler.ObjIdCounter < numTasks/2 {
			toMake = scheduler.ScheduleTask(1, []string{"tempipt1"}, objMap)
			expObjLenMap[strconv.Itoa(toMake)] = nPrimes
		} else {
			//for added complexity, schedule a series of tasks requiring promises on past tasks
			promisedObj := strconv.Itoa(outputObjects[count])
			count += 1

			toMake = scheduler.ScheduleTask(2, []string{"tempipt1", promisedObj}, objMap)
			if nNum < expObjLenMap[promisedObj] {
				expObjLenMap[strconv.Itoa(toMake)] = nNum
			} else {
				expObjLenMap[strconv.Itoa(toMake)] = expObjLenMap[promisedObj]
			}
		}
		outputObjects = append(outputObjects, toMake)
	}
	time.Sleep(1000 * time.Millisecond)
	//get answers back
	var wg sync.WaitGroup
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(i int) {
			target := strconv.Itoa(outputObjects[i])
			byteAns, err := scheduler.RetrieveObject(target)
			assert.Nil(t, err)
			ans := hoplite.BytesToUInt64Arr(byteAns)

			assert.True(t, len(ans) == expObjLenMap[target])
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestSchedulerFiveNodesConcurrent(t *testing.T) {
	numTasks := 50
	setup := MakeTestSetup(MakeFiveNodes())
	LaunchConcurrentTasksScheduler(t, setup, numTasks)
}
