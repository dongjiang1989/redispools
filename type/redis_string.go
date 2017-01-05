package Redis

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	DefaultStringKey = "StringRedis"
)

// NewStringRedis returns a new RedisType.
func NewStringRedis() RedisType {
	rds := RedisString{rtype: "string", keys: DefaultRedisCacheKey}
	return &rds
}

type RedisString struct {
	rtype string // type "string"
	keys  string // string type: hash key
}

// Get from redis by string.
func (rs *RedisString) Get(key string) interface{} {
	if v, err := redisPool.do("GET", key); err == nil {
		return v
	}
	return nil
}

// Set Interface error
func (rs *RedisString) GetSub(key, subKey string) interface{} {
	return nil
}

// GetMulti get from redis by string.
func (rs *RedisString) GetMulti(keys []string) []interface{} {
	size := len(keys)
	var rv []interface{}
	var c redis.Conn
	ts := time.Now().UnixNano()
	numServers := int64(len(redisPool.p))
	for i := int64(0); i < numServers; i++ {
		id := (ts + i) % numServers
		c = redisPool.p[id].Get()
		defer c.Close()
	}
	var err error
	for _, key := range keys {
		err = c.Send("GET", key)
		if err != nil {
			goto ERROR
		}
	}
	if err = c.Flush(); err != nil {
		goto ERROR
	}
	for i := 0; i < size; i++ {
		if v, err := c.Receive(); err == nil {
			rv = append(rv, v.([]byte))
		} else {
			rv = append(rv, err)
		}
	}
	return rv

ERROR:
	rv = rv[0:0]
	for i := 0; i < size; i++ {
		rv = append(rv, nil)
	}

	return rv
}

// Put put to redis from string.
func (rc *RedisString) Put(key string, val interface{}) error {
	var err error
	if _, err = redisPool.do("SET", key, val); err != nil {
		return err
	}

	if _, err = redisPool.do("HSET", rc.keys, key, "string"); err != nil {
		return err
	}
	return err
}

// empty Interface
func (rc *RedisString) PutSub(key string, subKey string, subVal interface{}) error {
	return errors.New("Redis string type No this function")
}

// Delete delete cache in redis.
func (rc *RedisString) Delete(key string) error {
	var err error
	if _, err = redisPool.do("DEL", key); err != nil {
		return err
	}
	_, err = redisPool.do("HDEL", rc.keys, key)
	return err
}

func (rc *RedisString) DeleteSub(key, subKey string) error {
	return errors.New("Redis string type No this function")
}

func (rc *RedisString) Len(key string) interface{} {
	return nil
}

// IsExist check cache's existence in redis with string.
func (rc *RedisString) IsExist(key string) bool {
	v, err := redis.Bool(redisPool.do("EXISTS", key))
	if err != nil {
		return false
	}
	if v == false {
		if _, err = redisPool.do("HDEL", rc.keys, key); err != nil {
			return false
		}
	}
	return v
}

// empty interface
func (rc *RedisString) IsExistSub(key, subKey string) bool {
	return false
}

// Incr increase counter in redis with string.
func (rs *RedisString) Incr(key string) error {
	_, err := redis.Bool(redisPool.do("INCRBY", key, 1))
	return err
}

func (rs *RedisString) IncrSub(key, subKey string) error {
	return errors.New("Redis string type No this function")
}
func (rs *RedisString) DecrSub(key, subKey string) error {
	return errors.New("Redis string type No this function")
}

// Decr decrease counter in redis with string.
func (rs *RedisString) Decr(key string) error {
	_, err := redis.Bool(redisPool.do("INCRBY", key, -1))
	return err
}

// ClearAll clean all cache in redis. delete this redis collection with string.
func (rs *RedisString) ClearAll() error {
	cachedKeys, err := redis.Strings(redisPool.do("HKEYS", rs.keys))
	if err != nil {
		return err
	}
	for _, str := range cachedKeys {
		if _, err = redisPool.do("DEL", str); err != nil {
			return err
		}
	}
	_, err = redisPool.do("DEL", rs.keys)
	return err
}

func (rs *RedisString) StartAndGC(config string) error {
	var cf map[string]string
	json.Unmarshal([]byte(config), &cf)

	if _, ok := cf["key"]; !ok {
		return errors.New("config has no key")
	}
	if _, ok := cf["rtype"]; !ok {
		cf["rtype"] = "string"
	}
	rs.rtype = cf["rtype"]
	rs.keys = cf["key"]

	return nil
}

func (rs *RedisString) GetRType() string {
	return rs.rtype
}

// Keys is not in redis string
func (rs *RedisString) Keys(key string) interface{} {
	return nil
}

// Values is not in redis string
func (rs *RedisString) Values(key string) interface{} {
	return nil
}

func init() {
	Register("string", NewStringRedis)
}
