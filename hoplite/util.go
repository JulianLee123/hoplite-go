package hoplite

import (
	"hash/fnv"
	"encoding/binary"
	"hoplite.go/hoplite/proto"
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

func CreateUpdateOdsInfo(size int, nodeName string, isComplete bool, odsInfo *proto.OdsInfo) *proto.OdsInfo {
	//craete odsInfo, or add new entry to odsInfo
	if odsInfo == nil{
		odsInfo = &proto.OdsInfo{Size: int64(size), LocationInfos: make(map[string]bool)}
	} else{
		if size > int(odsInfo.Size){
			odsInfo.Size = int64(size)
		}
	}
	odsInfo.LocationInfos[nodeName] = isComplete
	return odsInfo
}

func RmOdsInfo(removalKey string, odsInfo *proto.OdsInfo){
	delete(odsInfo.LocationInfos, removalKey)
}

func IsOdsEq(odsInfo1 *proto.OdsInfo, odsInfo2 *proto.OdsInfo) bool{
	if odsInfo1.Size != odsInfo2.Size || len(odsInfo1.LocationInfos) != len(odsInfo2.LocationInfos){
		return false
	}
	for k, v := range odsInfo1.LocationInfos { 
		v2, exists := odsInfo2.LocationInfos[k]
		if !exists || v2 != v {
			return false
		}
	}
	return true
}