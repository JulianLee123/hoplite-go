package hoplitetest

import (
	//"fmt"
	"testing"
	"math/rand"
	"time"
	"strings"
	"sync"
	"runtime"

	"hoplite.go/hoplite"
	"github.com/stretchr/testify/assert"
)

func TestOdsServerOneNode(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	
	_, wasFound, err := setup.OdsGet("n1", "obj1")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	odsInfo := hoplite.CreateUpdateOdsInfo(10, "n1", true, nil)
	err = setup.OdsSet("n1", "obj1", odsInfo)
	assert.Nil(t, err)

	val, wasFound, err := setup.OdsGet("n1", "obj1")
	assert.True(t, wasFound)
	assert.True(t, hoplite.IsOdsEq(odsInfo, val))
	assert.Nil(t, err)
	val, wasFound, err = setup.OdsGet("n1", "obj1")
	assert.True(t, wasFound)
	assert.True(t, hoplite.IsOdsEq(odsInfo, val))
	assert.Nil(t, err)

	err = setup.OdsDelete("n1", "obj1", "n1")
	assert.Nil(t, err)

	_, wasFound, err = setup.OdsGet("n1", "obj1")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestOdsServerOneNode2(t *testing.T) {
	//tests multi-entry adds to info associated with a single object
	setup := MakeTestSetup(MakeBasicOneShard())

	odsInfo := hoplite.CreateUpdateOdsInfo(10, "n1", true, nil)
	err := setup.OdsSet("n1", "obj1", odsInfo)
	assert.Nil(t, err)

	odsInfo2 := hoplite.CreateUpdateOdsInfo(12, "n2", true, nil)
	err = setup.OdsSet("n1", "obj1", odsInfo2)
	assert.Nil(t, err)

	odsInfo = hoplite.CreateUpdateOdsInfo(12, "n2", true, odsInfo) //odsInfo + odsInfo2

	val, wasFound, err := setup.OdsGet("n1", "obj1")
	assert.True(t, wasFound)
	assert.True(t, hoplite.IsOdsEq(odsInfo, val))
	assert.Nil(t, err)

	//delete should delete only the specified entry
	err = setup.OdsDelete("n1", "obj1", "n1")
	assert.Nil(t, err)
	val, wasFound, err = setup.OdsGet("n1", "obj1")
	assert.True(t, wasFound)
	assert.True(t, hoplite.IsOdsEq(odsInfo2, val))
	assert.Nil(t, err)

	//now everything gone
	err = setup.OdsDelete("n1", "obj1", "n2")
	assert.Nil(t, err)
	_, wasFound, err = setup.OdsGet("n1", "obj1")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func randomString(rng *rand.Rand, length int) string {
	chars := "abcdefghijklmnopqrstuvwxyz"

	out := strings.Builder{}
	for i := 0; i < length; i++ {
		out.WriteByte(chars[rng.Int()%len(chars)])
	}
	return out.String()
}

func RandomKeys(n, length int) []string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	out := make([]string, 0)
	for i := 0; i < n; i++ {
		out = append(out, randomString(rng, length))
	}
	return out
}

func RunConcurrentGetsAndSets(t *testing.T, setup *TestSetup) {
	keys := RandomKeys(200, 10)
	odsInfo := hoplite.CreateUpdateOdsInfo(10, "n1", true, nil)
	for _, k := range keys {
		err := setup.OdsSet("n1", k, odsInfo)
		assert.Nil(t, err)
	}

	var wg sync.WaitGroup
	goros := runtime.NumCPU() * 2
	readIters := 1//20
	writeIters := 10
	odsInfoSum := hoplite.CreateUpdateOdsInfo(10, "n2", true, odsInfo)
	odsInfo = hoplite.CreateUpdateOdsInfo(10, "n1", true, nil)//recreate object
	odsInfo2 := hoplite.CreateUpdateOdsInfo(10, "n2", true, nil)
	for i := 0; i < goros; i++ {
		wg.Add(2)
		// start a writing goro
		go func() {
			for j := 0; j < writeIters; j++ {
				for _, k := range keys {
					err := setup.OdsSet("n1", k, odsInfo2)
					assert.Nil(t, err)
				}
			}
			wg.Done()
		}()
		// start a reading goro
		go func() {
			for j := 0; j < readIters; j++ {
				for _, k := range keys {
					val, wasFound, err := setup.OdsGet("n1", k)
					assert.Nil(t, err)
					assert.True(t, wasFound)
					assert.True(t,hoplite.IsOdsEq(odsInfo, val) || hoplite.IsOdsEq(odsInfoSum, val))
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestOdsServerConcurrentGetsSets(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	RunConcurrentGetsAndSets(t, setup)
}

func TestOdsServerConcurrentGetsSetsMultiShard(t *testing.T) {
	setup := MakeTestSetup(MakeBasicMultiShard())
	RunConcurrentGetsAndSets(t, setup)
}