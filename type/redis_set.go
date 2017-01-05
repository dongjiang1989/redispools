package Redis

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

// NewSetRedis returns a new RedisType.
func NewSetRedis() RedisType {
	rds := RedisSet{rtype: "set", keys: DefaultRedisCacheKey}
	return &rds
}

type RedisSet struct {
	rtype string // type "set"
	keys  string // string type: set keys
}

// Get from redis by set.
func (rs *RedisSet) Get(key string) interface{} {
	if v, err := redisPool.do("SMEMBERS", key); err == nil {
		return v
	}
	return nil
}

// Get subKey from redis by set.
func (rs *RedisSet) GetSub(key, subKey string) interface{} {
	return nil
}

// GetMulti get from redis by set.
func (rs *RedisSet) GetMulti(keys []string) []interface{} {
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
		err = c.Send("SMEMBERS", key)
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

// Put put to redis from set.
func (rc *RedisSet) Put(key string, val interface{}) error {
	var err error
	tmp := val.([]string)
	if len(tmp) == 0 {
		return nil
	}
	var input []interface{}
	input = append(input, key)
	for _, v := range tmp {
		input = append(input, v)
	}
	if _, err = redisPool.do("SADD", input...); err != nil {
		return err
	}

	if _, err = redisPool.do("HSET", rc.keys, key, "set"); err != nil {
		return err
	}
	return err
}

// Put put to redis from set.
func (rc *RedisSet) PutSub(key string, subKey string, subVal interface{}) error {
	return errors.New("Redis Set is not this function!")
}

// Delete delete cache in redis.
func (rc *RedisSet) Delete(key string) error {
	var err error
	if _, err = redisPool.do("DEL", key); err != nil {
		return err
	}
	_, err = redisPool.do("HDEL", rc.keys, key)
	return err
}

// Delete delete subKey in redis.
func (rc *RedisSet) DeleteSub(key, subKey string) error {
	var err error
	if _, err = redisPool.do("SREM", key, subKey); err != nil {
		return err
	}
	if lenth, err := redisPool.do("SCARD", key); err == nil && lenth.(int64) == 0 {
		_, err = redisPool.do("HDEL", rc.keys, key)
	}
	return err
}

// Len in redis Set
func (rc *RedisSet) Len(key string) interface{} {
	v, err := redisPool.do("SCARD", key)
	if err == nil {
		return v
	}
	return nil
}

// IsExist check cache's existence in redis with Hash.
func (rc *RedisSet) IsExist(key string) bool {
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
func (rc *RedisSet) IsExistSub(key, subKey string) bool {
	v, err := redis.Bool(redisPool.do("EXISTS", key))
	if err != nil {
		return false
	}

	if v == true {
		v1, err := redis.Bool(redisPool.do("SISMEMBER", key, subKey))
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

// Incr increase counter in redis with Set.
func (rs *RedisSet) Incr(key string) error {
	return errors.New("Redis Set is not Incr!")
}

// Incr increase counter in redis with Set.
func (rs *RedisSet) IncrSub(key, subKey string) error {
	return errors.New("Redis Set is not IncrSub!")
}

// Decr decrease counter in redis with hash.
func (rs *RedisSet) Decr(key string) error {
	return errors.New("Redis Set is not Decr!")
}

// Decr decrease counter in redis with hash.
func (rs *RedisSet) DecrSub(key, subKey string) error {
	return errors.New("Redis Set is not DecrSub!")
}

// ClearAll clean all cache in redis. delete this redis collection with string.
func (rs *RedisSet) ClearAll() error {
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

func (rs *RedisSet) StartAndGC(config string) error {
	var cf map[string]string
	json.Unmarshal([]byte(config), &cf)

	if _, ok := cf["key"]; !ok {
		return errors.New("config has no key")
	}
	if _, ok := cf["rtype"]; !ok {
		cf["rtype"] = "set"
	}
	rs.rtype = cf["rtype"]
	rs.keys = cf["key"]

	return nil
}

func (rs *RedisSet) GetRType() string {
	return rs.rtype
}

// Keys is not in redis set
func (rs *RedisSet) Keys(key string) interface{} {
	return nil
}

// Values is not in redis set
func (rs *RedisSet) Values(key string) interface{} {
	return nil
}

func init() {
	Register("set", NewSetRedis)
}
