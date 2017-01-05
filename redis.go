package Redis

import (
	"common/redis/type"

	log "github.com/cihub/seelog"

	"github.com/garyburd/redigo/redis"
)

type RedisInstance struct {
	CacheKey string
	Adapters map[string]Redis.RedisType
}

func (r *RedisInstance) NewRedisInstance(cachekey string) error {
	r.CacheKey = cachekey
	r.Adapters = make(map[string]Redis.RedisType)

	adapter, err := Redis.RedisTypeInstance("string", `{"rtype":"string", "key":"`+cachekey+`"}`)
	if err != nil {
		return err
	} else {
		if _, ok := r.Adapters["string"]; ok {
			log.Error("RedisInstance: Redis Instance string type")
		} else {
			r.Adapters["string"] = adapter
		}
	}

	adapter, err = Redis.RedisTypeInstance("hash", `{"rtype":"hash", "key":"`+cachekey+`"}`)
	if err != nil {
		return err
	} else {
		r.Adapters["hash"] = adapter
	}

	adapter, err = Redis.RedisTypeInstance("set", `{"rtype":"set", "key":"`+cachekey+`"}`)
	if err != nil {
		return err
	} else {
		r.Adapters["set"] = adapter
	}

	return nil
}

func (r *RedisInstance) ClearAll() error {
	return Redis.ClearAll(r.CacheKey)
}

func (r *RedisInstance) GetCacheKeys() (map[string]string, error) {
	return redis.StringMap(r.Adapters["hash"].Get(r.CacheKey), nil)
}
