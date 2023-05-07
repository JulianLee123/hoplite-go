package hoplitetest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOneNodeError(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())
	_, err := setup.Get("abc", "n1")
	assert.NotNil(t, err)
}
