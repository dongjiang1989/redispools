package Redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RedisPool(t *testing.T) {
	NewInit(`{"conn":"127.0.0.1:6379", "count": "10", "password": ""}`)

	assert := assert.New(t)
	assert.Equal(redisPool.p[0].MaxIdle, 10, "is not 10")
	assert.NotNil(redisPool.p, "is nil")
}
