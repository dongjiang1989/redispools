package Redis

import (
	"common/redis/type"
	"errors"
)

func (It *Item) SetItemKey(value interface{}) error {
	sr, err := Redis.RedisTypeInstance("set", `{"rtype":"set", "key":"SetCacheKey"}`)
	if err != nil {
		return err
	}

	hr, err := Redis.RedisTypeInstance("hash", `{"rtype":"hash", "key":"HashCacheKey"}`)
	if err != nil {
		return err
	}

	str, err := Redis.RedisTypeInstance("string", `{"rtype":"string", "key":"HashCacheKey"}`)
	if err != nil {
		return err
	}

	if It.Rtype == sr.GetRType() {
		return sr.Put(It.Key, value)
	}

	if It.Rtype == hr.GetRType() {
		return hr.Put(It.Key, value)
	}

	if It.Rtype == str.GetRType() {
		return str.Put(It.Key, value)
	}

	return errors.New("Set Item to redis error!")
}
