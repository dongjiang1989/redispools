package Redis

import (
	"fmt"
)

var (
	DefaultRedisCacheKey = "RedisCacheKey"
)

type RedisType interface {
	//------------------------------------------------------------
	// Common function interface
	// get Redis type
	GetRType() string
	// get redis value by key.
	Get(key string) interface{}
	// set redis value with key.
	Put(key string, val interface{}) error
	// delete redis value by key.
	Delete(key string) error
	// increase redis int value by key, as a counter.
	Incr(key string) error
	// decrease redis int value by key, as a counter.
	Decr(key string) error
	// check if redis value exists or not.
	IsExist(key string) bool
	//clear all redis.
	//ClearAll() error
	// start gc routine based on config string settings.
	StartAndGC(config string) error

	//------------------------------------------------------------
	// Just for redis hash
	GetSub(key, subKey string) interface{}
	PutSub(key string, subKey string, subVal interface{}) error
	IsExistSub(key, subKey string) bool
	DeleteSub(key, subKey string) error
	IncrSub(key, subKey string) error
	DecrSub(key, subKey string) error
	Len(key string) interface{}
	Keys(key string) interface{}
	Values(key string) interface{}
	//------------------------------------------------------------

}

// Instance is a function create a new RedisType Instance
type Instance func() RedisType

var adapters = make(map[string]Instance)

func Register(name string, adapter Instance) {
	if adapter == nil {
		panic("Redis: Register adapter is nil")
	}
	if _, ok := adapters[name]; ok {
		panic("Redis: Register called twice for adapter " + name)
	}
	adapters[name] = adapter
}

func RedisTypeInstance(adapterName, config string) (adapter RedisType, err error) {
	instanceFunc, ok := adapters[adapterName]
	if !ok {
		err = fmt.Errorf("RedisType: unknown adapter name %q (forgot to import?)", adapterName)
		return
	}
	adapter = instanceFunc()
	err = adapter.StartAndGC(config)
	if err != nil {
		adapter = nil
	}
	return
}
