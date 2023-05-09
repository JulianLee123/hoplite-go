package hoplitetest

import (
	"fmt"
	"strconv"
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
	for i := range primes {
		fmt.Println(primes[i])
	}
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
	for i := range primes {
		fmt.Println(primes[i])
	}
	assert.Equal(t, len(primes), 3)
}

func TestBasicScheduleOneTwoNodes(t *testing.T) {
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
	objectMap["0"] = byteArray1
	objectMap["-1"] = byteArray2
	result1 := scheduler.ScheduleTask(1, []string{"0"}, objectMap)
	result2 := scheduler.ScheduleTask(1, []string{"-1"}, objectMap)

	result := scheduler.ScheduleTask(2, []string{strconv.Itoa(result1), strconv.Itoa(result2)}, objectMap)

	time.Sleep(100 * time.Millisecond)

	val, err := scheduler.RetrieveObject(strconv.Itoa(result))

	assert.Nil(t, err)

	primes := hoplite.BytesToUInt64Arr(val)
	for i := range primes {
		fmt.Println(primes[i])
	}
	assert.Equal(t, len(primes), 6)
}
