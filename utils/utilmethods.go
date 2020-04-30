package utils

import (
	"encoding/binary"
	"fmt"

	"github.com/garyburd/redigo/redis"
)

func B2S(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}

func BytesToInt32(bs []uint8) int32 {
	b := make([]byte, len(bs))
	return int32(binary.BigEndian.Uint32(b))
}

func Transfer(redisRtn interface{}) []interface{} {
	rtn, error := redis.Values(redisRtn, nil)
	if error != nil {
		fmt.Println("redis.Values method error.")
	}
	return rtn
}
