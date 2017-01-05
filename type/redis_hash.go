package Redis

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

// NewHashRedis returns a new RedisType.
func NewHashRedis() RedisType {
	rds := RedisHash{rtype: "hash", keys: DefaultRedisCacheKey}
	return &rds
}

type RedisHash struct {
	rtype string // type "hash"
	keys  string // string type: hash keys
}

// Get from redis by hash.
func (rs *RedisHash) Get(key string) interface{} {
	if v, err := redisPool.do("HGETALL", key); err == nil {
		return v
	}
	return nil
}

// Get subKey from redis by hash.
func (rs *RedisHash) GetSub(key, subKey string) interface{} {
	if subKey == "" {
		return nil
	}
	if v, err := redisPool.do("HGET", key, subKey); err == nil {
		return v
	}
	return nil
}

// GetMulti get from redis by hash.
func (rs *RedisHash) GetMulti(keys []string) []interface{} {
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
		err = c.Send("HGETALL", key)
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

// Put put to redis from hash.
func (rc *RedisHash) Put(key string, val interface{}) error {
	var err error
	tmp := val.(map[string]string)
	if len(tmp) == 0 {
		return nil
	}
	var input []interface{}
	input = append(input, key)
	for k, v := range tmp {
		input = append(input, k)
		input = append(input, v)
	}
	if _, err = redisPool.do("HMSET", input...); err != nil {
		return err
	}

	if _, err = redisPool.do("HSET", rc.keys, key, "hash"); err != nil {
		return err
	}
	return err
}

// Put put to redis from Hash.
func (rc *RedisHash) PutSub(key string, subKey string, subVal interface{}) error {
	var err error
	if _, err = redisPool.do("HSET", key, subKey, subVal); err != nil {
		return err
	}

	if _, err = redisPool.do("HSET", rc.keys, key, "hash"); err != nil {
		return err
	}
	return err
}

// Delete delete cache in redis.
func (rc *RedisHash) Delete(key string) error {
	var err error
	if _, err = redisPool.do("DEL", key); err != nil {
		return err
	}
	_, err = redisPool.do("HDEL", rc.keys, key)
	return err
}

// Delete delete subKey in redis.
func (rc *RedisHash) DeleteSub(key, subKey string) error {
	var err error
	if _, err = redisPool.do("HDEL", key, subKey); err != nil {
		return err
	}
	if lenth, err := redisPool.do("HLEN", key); err == nil && lenth.(int64) == 0 {
		_, err = redisPool.do("HDEL", rc.keys, key)
	}
	return err
}

// Len in redis hash
func (rc *RedisHash) Len(key string) interface{} {
	v, err := redisPool.do("HLEN", key)
	if err == nil {
		return v
	}
	return nil
}

// IsExist check cache's existence in redis with Hash.
func (rc *RedisHash) IsExist(key string) bool {
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

// IsExist check cache's existence in redis with Hash.
func (rc *RedisHash) IsExistSub(key, subKey string) bool {
	v, err := redis.Bool(redisPool.do("EXISTS", key))
	if err != nil {
		return false
	}

	if v == true {
		v1, err := redis.Bool(redisPool.do("HEXISTS", key, subKey))
		if err != nil {
			return false
		}
		return v1
	} else {
		if _, err = redisPool.do("HDEL", rc.keys, key); err != nil {
			return false
		}
	}
	return v
}

// Incr increase counter in redis with hash.
func (rs *RedisHash) Incr(key string) error {
	_, err := redis.Bool(redisPool.do("INCRBY", key, 1))
	return err
}

// Incr increase counter in redis with hash.
func (rs *RedisHash) IncrSub(key, subKey string) error {
	_, err := redis.Bool(redisPool.do("HINCRBY", key, subKey, 1))
	return err
}

// Decr decrease counter in redis with hash.
func (rs *RedisHash) Decr(key string) error {
	_, err := redis.Bool(redisPool.do("INCRBY", key, -1))
	return err
}

// Decr decrease counter in redis with hash.
func (rs *RedisHash) DecrSub(key, subKey string) error {
	_, err := redis.Bool(redisPool.do("HINCRBY", key, subKey, -1))
	return err
}

// ClearAll clean all cache in redis. delete this redis collection with string.
func (rs *RedisHash) ClearAll() error {
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

func (rs *RedisHash) StartAndGC(config string) error {
	var cf map[string]string
	json.Unmarshal([]byte(config), &cf)

	if _, ok := cf["key"]; !ok {
		return errors.New("config has no key")
	}
	if _, ok := cf["rtype"]; !ok {
		cf["rtype"] = "hash"
	}
	rs.rtype = cf["rtype"]
	rs.keys = cf["key"]

	return nil
}

func (rs *RedisHash) GetRType() string {
	return rs.rtype
}

// Keys is in redis hash
func (rs *RedisHash) Keys(key string) interface{} {
	if v, err := redisPool.do("HKEYS", key); err == nil {
		return v
	}
	return nil
}

// Values is in redis hash
func (rs *RedisHash) Values(key string) interface{} {
	if v, err := redisPool.do("HVALS", key); err == nil {
		return v
	}
	return nil
}

func init() {
	Register("hash", NewHashRedis)
}
