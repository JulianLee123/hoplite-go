package hoplite

import (
	"hash/fnv"
	"encoding/binary"
)

/// This file can be used for any common code you want to define and separate
/// out from server.go or client.go

var usingTtl = false

func GetShardForKey(key string, numShards int) int {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return int(hasher.Sum32())%numShards + 1
}

func BytesToUInt64Arr(byteArr []byte) []uint64 {
	var intArr []uint64
	i := 0 
	for i < len(byteArr){
		intArr = append(intArr, binary.LittleEndian.Uint64(byteArr[i:i+8]))
		i += 8
	}
	return intArr
}

func UInt64ToBytesArr(intArr []uint64) []byte {
	byteArr := make([]byte, 8 * len(intArr))
	i := 0
	for i < len(byteArr){
		binary.LittleEndian.PutUint64(byteArr[i:i+8], uint64(i))
		i += 8
	}
	return byteArr
}