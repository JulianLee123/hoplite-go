package hoplitetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"hoplite.go/hoplite"
)

func TestBytesToIntArr(t *testing.T) {
	x := []uint64{1, 2, 3, 4, 5, 6}
	y := hoplite.UInt64ToBytesArr(x)
	z := hoplite.BytesToUInt64Arr(y)

	assert.Equal(t, len(z), 6)

}
