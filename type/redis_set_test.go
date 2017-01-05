package Redis

import (
	"log"
	"sort"
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func Test_RedisSet(t *testing.T) {
	NewInit(`{"conn":"127.0.0.1:6379"}`)

	assert := assert.New(t)

	bm, err := RedisTypeInstance("set", `{"rtype":"set", "key":"ADfqdvcrtsscd"}`)
	assert.Nil(err, "init err")

	assert.Equal(bm.GetRType(), "set", "type is not equal")

	err = bm.Put("dongjiang", []string{"aa", "a", "bb", "1"})
	assert.Nil(err, "set err")

	assert.True(bm.IsExist("dongjiang"), "check err")
	assert.True(bm.IsExistSub("dongjiang", "aa"), "check err")
	assert.True(bm.IsExistSub("dongjiang", "bb"), "check err")
	assert.False(bm.IsExistSub("dongjiang", "not find"), "check err")
	assert.False(bm.IsExistSub("dongjiang", ""), "check err")

	v, _ := redis.Strings(bm.Get("not expct"), err)
	assert.Equal(v, []string{}, "error is not []")

	log.Println(bm.Get("dongjiang"))
	v, _ = redis.Strings(bm.Get("dongjiang"), err)
	//TODO no match must sort
	sort.Strings(v)
	aaa := []string{"aa", "a", "bb", "1"}
	sort.Strings(aaa)
	assert.Equal(v, aaa, "error is not []")

	// bad case
	//	log.Println(redis.StringMap(bm.Get("dongjiang"), err))
	//	v1, _ := redis.StringMap(bm.Get("dongjiang"), err)
	//	aaa = []string{"aa", "a", "bb", "1"}
	//	sort.Strings(aaa)
	//	assert.Equal(v1, aaa, "error is not []")

	log.Println(bm.GetSub("dongjiang", "aa"))
	v2, _ := redis.String(bm.GetSub("dongjiang", "aa"), err)
	assert.Equal(v2, "", "error is not []")

	err = bm.Put("dongjiang", []string{"cc", "d", "bb", "2"})

	v3, _ := redis.Int(bm.Len("dongjiang"), err)
	assert.Equal(v3, 7, "get count is error")

	err = bm.Incr("dongjiang")
	assert.NotNil(err, "Incr err")

	err = bm.IncrSub("dongjiang", "aa")
	assert.NotNil(err, "Incr err")

	err = bm.IncrSub("dongjiang", "bb")
	assert.NotNil(err, "Incr err")

	err = bm.DecrSub("dongjiang", "bb")
	assert.NotNil(err, "Incr err")

	err = bm.DecrSub("dongjiang", "bb")
	assert.NotNil(err, "Incr err")

	err = bm.DeleteSub("dongjiang", "aaaa")
	assert.Nil(err, "Incr err")

	assert.True(bm.IsExistSub("dongjiang", "aa"), "check err")

	err = bm.DeleteSub("dongjiang", "aa")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "aa"))
	v6, _ := redis.String(bm.GetSub("dongjiang", "aa"), err)
	assert.Equal(v6, "", "error is not []")

	assert.False(bm.IsExistSub("dongjiang", "aa"), "check err")

	err = bm.DeleteSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v6, _ = redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v6, "", "error is not []")

	err = bm.DeleteSub("dongjiang", "cc")
	assert.Nil(err, "Incr err")

	cvb, _ := redis.Strings(bm.Keys("dongjiang"), nil)
	assert.Nil(cvb, "errr")

	err = bm.DeleteSub("dongjiang", "1")
	assert.Nil(err, "Incr err")
	err = bm.DeleteSub("dongjiang", "2")
	assert.Nil(err, "Incr err")
	err = bm.DeleteSub("dongjiang", "a")
	assert.Nil(err, "Incr err")
	err = bm.DeleteSub("dongjiang", "d")
	assert.Nil(err, "Incr err")

	log.Println("dongjiang", bm.Get("dongjiang"))

	assert.False(bm.IsExist("dongjiang"), "check err")

	// test clear all
	err = ClearAll("ADfqdvcrtsscd")
	assert.Nil(err, "clear all err")
}
