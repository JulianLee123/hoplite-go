package hoplitetest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneNodeError(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	_, err := setup.Get("abc", "n1")
	assert.NotNil(t, err)
}

func TestOneNodeSuccess(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	setup.Set("abc", "n1")
	_, err := setup.Get("abc", "n1")
	assert.NotNil(t, err)
}

func TestTwoNodeSuccess(t *testing.T) {
	setup := MakeTestSetup(MakeBasicTwoShard())

	_, err := setup.Set("abc", "n2")
	fmt.Println(err)
	val2, err2 := setup.Get("abc", "n2")
	fmt.Println(err2)
	fmt.Println(val2.Value)
	fmt.Println(val2.WasFound)
	assert.NotNil(t, err)
}
