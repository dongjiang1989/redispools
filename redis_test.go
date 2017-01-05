package Redis

import (
	"common/redis/type"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Redis_test(t *testing.T) {
	Redis.NewInit(`{"conn":"127.0.0.1:6379", "count": "10", "password": ""}`)
	assert := assert.New(t)

	r := new(RedisInstance)

	err := r.NewRedisInstance("dongjiang")
	assert.Nil(err, "is not nil")

	aaa, err := r.GetCacheKeys()
	assert.Nil(err, "is not nil")
	assert.Equal(len(aaa), 0, "is not nil")

	err = r.ClearAll()
	assert.Nil(err, "is not nil")

	aaa, err = r.GetCacheKeys()
	assert.Nil(err, "is not nil")
	assert.Equal(len(aaa), 0, "is not nil")

}
