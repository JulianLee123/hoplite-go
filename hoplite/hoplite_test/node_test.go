package hoplitetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneNode(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	_, err := setup.Get("abc", "n1")
	assert.Nil(t, err)
}
