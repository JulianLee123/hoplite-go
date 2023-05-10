package hoplitetest

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"hoplite.go/hoplite"
)

func LaunchConcurrentTasksBenchmark(setup *TestSetup, nodeNames []string, numTasks int, slowTasks bool) {
	targetTaskId := 2
	//test processing n concurrent tasks and concurrent answer fetches, make sure answers are valid
	expObjLenMap := make(map[string]int) //expected lengths for produced objects
	for i := 0; i < numTasks; i++ {
		//schedule task asynchronous: don't need to launch separate goroutine
		objToMake := fmt.Sprintf("%s%d", "obj", i)
		nNum := rand.Int()%100 + 500
		nPrimes := rand.Int()%nNum + 1
		byteArr := GenIptBytesArr(nNum, nPrimes, slowTasks)
		objMap := make(map[string][]byte)
		objMap["tempipt1"] = byteArr
		if i < numTasks/2 {
			setup.ScheduleTask(nodeNames[rand.Int()%len(nodeNames)], objToMake, 1, []string{"tempipt1"}, objMap)
			expObjLenMap[objToMake] = nPrimes
		} else {
			//for added complexity, schedule a series of tasks requiring promises on past tasks
			promisedObj := fmt.Sprintf("%s%d", "obj", i-numTasks/2)
			setup.ScheduleTask(nodeNames[rand.Int()%len(nodeNames)], objToMake, targetTaskId, []string{"tempipt1", promisedObj}, objMap)
			if nNum < expObjLenMap[promisedObj] {
				expObjLenMap[objToMake] = nNum
			} else {
				expObjLenMap[objToMake] = expObjLenMap[promisedObj]
			}
		}
	}
	//get answers back
	var wg sync.WaitGroup
	for i := 0; i < numTasks; i++ {
		objToFind := fmt.Sprintf("%s%d", "obj", i)
		wg.Add(1)
		go func(i int) {
			byteAns, _ := setup.GetTaskAns(nodeNames[rand.Int()%len(nodeNames)], objToFind)
			hoplite.BytesToUInt64Arr(byteAns)
			print(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
	//delete all objects to save space
	for i := 0; i < numTasks; i++ {
		objToDel := fmt.Sprintf("%s%d", "obj", i)
		setup.DeleteGlobalObj(nodeNames[rand.Int()%len(nodeNames)], objToDel)
	}
	time.Sleep(400 * time.Millisecond) //wait for objects to be deleted
	//make sure objects no longer there: expect GetTaskAns to stall
	for i := 0; i < numTasks; i++ {
		objToFind := fmt.Sprintf("%s%d", "obj", i)
		go func(i int) {
			setup.GetTaskAns(nodeNames[rand.Int()%len(nodeNames)], objToFind)
		}(i)
	}
	time.Sleep(400 * time.Millisecond) //make sure objects aren't there
}

func BenchmarkConcurrentFastTasksFiveNodes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		numTasks := 12
		setup := MakeTestSetup(MakeBasicFiveNodes())
		LaunchConcurrentTasksBenchmark(setup, []string{"n1", "n2", "n3", "n4", "n5"}, numTasks, false)
	}
}

func BenchmarkConcurrentSlowTasksFiveNodes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		numTasks := 12
		setup := MakeTestSetup(MakeBasicFiveNodes())
		LaunchConcurrentTasksBenchmark(setup, []string{"n1", "n2", "n3", "n4", "n5"}, numTasks, true)
	}
}