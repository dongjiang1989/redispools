package Redis

import (
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestCache_RedisString(t *testing.T) {
	NewInit(`{"conn":"127.0.0.1:6379"}`)

	assert := assert.New(t)

	bm, err := RedisTypeInstance("string", `{"rtype":"string", "key":"TestDongjiangABC"}`)
	assert.Nil(err, "init err")

	assert.Equal(bm.GetRType(), "string", "type is not equal")

	err = bm.Put("dongjiang", 1)
	assert.Nil(err, "set err")

	assert.True(bm.IsExist("dongjiang"), "check err")

	err = bm.Put("dongjiang", 1)
	assert.Nil(err, "set err")

	v, _ := redis.Int(bm.Get("dongjiang"), err)
	assert.Equal(v, 1, "get err")

	err = bm.Incr("dongjiang")
	assert.Nil(err, "Incr err")

	v, _ = redis.Int(bm.Get("dongjiang"), err)
	assert.Equal(v, 2, "get err")

	err = bm.Decr("dongjiang")
	assert.Nil(err, "Decr err")

	v, _ = redis.Int(bm.Get("dongjiang"), err)
	assert.Equal(v, 1, "get err")

	bm.Delete("dongjiang")
	assert.False(bm.IsExist("dongjiang"), "check err")

	//test string
	err = bm.Put("key", "value")
	assert.Nil(err, "set err")

	assert.True(bm.IsExist("key"), "check err")

	cvb, _ := redis.Strings(bm.Keys("dongjiang"), nil)
	assert.Nil(cvb, "errr")

	v1, _ := redis.String(bm.Get("key"), err)
	assert.Equal(v1, "value", "get err")

	err = bm.Put("key1", "value1")
	assert.Nil(err, "set err")

	assert.True(bm.IsExist("key1"), "check err")

	// test clear all
	err = ClearAll("TestDongjiangABC")
	assert.Nil(err, "clear all err")
}
