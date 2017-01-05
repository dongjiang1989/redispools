package Redis

import (
	"log"
	"sort"
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func Test_RedisHash(t *testing.T) {
	NewInit(`{"conn":"127.0.0.1:6379"}`)

	assert := assert.New(t)

	bm, err := RedisTypeInstance("hash", `{"rtype":"hash", "key":"hdfghzvwadsfasdf"}`)
	assert.Nil(err, "init err")

	assert.Equal(bm.GetRType(), "hash", "type is not equal")

	err = bm.Put("dongjiang", map[string]string{"aa": "a", "bb": "1"})
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
	sort.Strings(v)
	aaa := []string{"aa", "a", "bb", "1"}
	sort.Strings(aaa)
	assert.Equal(v, aaa, "error is not []")

	log.Println(redis.StringMap(bm.Get("dongjiang"), err))
	v1, _ := redis.StringMap(bm.Get("dongjiang"), err)
	assert.Equal(v1, map[string]string{"aa": "a", "bb": "1"}, "error is not []")

	log.Println(bm.GetSub("dongjiang", "aa"))
	v2, _ := redis.String(bm.GetSub("dongjiang", "aa"), err)
	assert.Equal(v2, "a", "error is not []")

	log.Println(bm.GetSub("dongjiang", "not find"))
	v2, _ = redis.String(bm.GetSub("dongjiang", "not find"), err)
	assert.Equal(v2, "", "error is not []")

	log.Println(bm.GetSub("not find", "aa"))
	v2, _ = redis.String(bm.GetSub("not find", "aa"), err)
	assert.Equal(v2, "", "error is not []")

	log.Println(bm.GetSub("dongjiang", ""))
	v2, _ = redis.String(bm.GetSub("dongjiang", ""), err)
	assert.Equal(v2, "", "error is not []")

	err = bm.Put("dongjiang", map[string]string{"cc": "d", "bb": "2"})

	v3, _ := redis.Int(bm.Len("dongjiang"), err)
	assert.Equal(v3, 3, "get count is error")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v4, _ := redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v4, "2", "error is not []")

	err = bm.Incr("dongjiang")
	assert.NotNil(err, "Incr err")

	err = bm.IncrSub("dongjiang", "aa")
	assert.NotNil(err, "Incr err")

	err = bm.IncrSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v5, _ := redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v5, "3", "error is not []")

	err = bm.DecrSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v5, _ = redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v5, "2", "error is not []")

	err = bm.DecrSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v5, _ = redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v5, "1", "error is not []")

	err = bm.DecrSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v5, _ = redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v5, "0", "error is not []")

	err = bm.DecrSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v5, _ = redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v5, "-1", "error is not []")

	err = bm.Decr("dongjiang")
	assert.NotNil(err, "Is err")

	err = bm.DeleteSub("dongjiang", "aaaa")
	assert.Nil(err, "Incr err")

	err = bm.DeleteSub("dongjiang", "aa")
	assert.Nil(err, "Incr err")

	log.Println(bm.GetSub("dongjiang", "aa"))
	v6, _ := redis.String(bm.GetSub("dongjiang", "aa"), err)
	assert.Equal(v6, "", "error is not []")

	err = bm.DeleteSub("dongjiang", "bb")
	assert.Nil(err, "Incr err")

	cvb, _ := redis.Strings(bm.Keys("dongjiang"), nil)
	assert.Equal(cvb, []string{"cc"}, "errr")

	log.Println(bm.GetSub("dongjiang", "bb"))
	v6, _ = redis.String(bm.GetSub("dongjiang", "bb"), err)
	assert.Equal(v6, "", "error is not []")

	err = bm.DeleteSub("dongjiang", "cc")
	assert.Nil(err, "Incr err")

	cvb, _ = redis.Strings(bm.Keys("dongjiang"), nil)
	assert.Equal(cvb, []string{}, "errr")

	log.Println(bm.GetSub("dongjiang", "cc"))
	v6, _ = redis.String(bm.GetSub("dongjiang", "cc"), err)
	assert.Equal(v6, "", "error is not []")

	assert.False(bm.IsExist("dongjiang"), "check err")

	// test clear all
	err = ClearAll("hdfghzvwadsfasdf")
	assert.Nil(err, "clear all err")
}
