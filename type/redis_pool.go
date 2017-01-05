package Redis

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	log "github.com/cihub/seelog"

	"github.com/garyburd/redigo/redis"
)

type RedisPool struct {
	p        []*redis.Pool // redis connection pool
	count    int
	conninfo []string
	password string
}

func (rc *RedisPool) do(commandName string, args ...interface{}) (reply interface{}, err error) {
	ts := time.Now().UnixNano()
	numServers := int64(len(rc.p))
	for i := int64(0); i < numServers; i++ {
		id := (ts + i) % numServers
		c := rc.p[id].Get()
		defer c.Close()
		return c.Do(commandName, args...)
	}
	return nil, errors.New("all redis servers err!")
}

// StartAndGC start redis cache adapter.
// config is like {"key":"collection key","conn":"connection info","dbNum":"0"}
// the cache item in redis are stored forever,
// so no gc operation.
func (rc *RedisPool) StartAndGC(config string) error {
	var cf map[string]string
	json.Unmarshal([]byte(config), &cf)

	if _, ok := cf["conn"]; !ok {
		return errors.New("config has no connection key")
	}
	if _, ok := cf["password"]; !ok {
		cf["password"] = ""
	}

	if _, ok := cf["count"]; !ok {
		cf["count"] = "3"
	}

	rc.conninfo = strings.Split(cf["conn"], ",")
	rc.password = cf["password"]
	count, err := strconv.Atoi(cf["count"])

	if err != nil {
		rc.count = 3
	} else {
		rc.count = count
	}

	rc.connectInit()

	ts := time.Now().UnixNano()
	numServers := int64(len(rc.p))
	for i := int64(0); i < numServers; i++ {
		id := (ts + i) % numServers
		c := rc.p[id].Get()
		defer c.Close()

		return c.Err()
	}

	return errors.New("Can not find a pool connection!")

}

// connect to redis.
func (rc *RedisPool) connectInit() {
	//clean
	rc.p = make([]*redis.Pool, 0)
	// initialize a new pool
	for _, conn := range rc.conninfo {
		p := &redis.Pool{
			MaxIdle:     rc.count,
			IdleTimeout: 180 * time.Second,
			Dial: func() (c redis.Conn, err error) {
				c, err = redis.Dial("tcp", conn)
				if err != nil {
					return nil, err
				}

				if rc.password != "" {
					if _, err := c.Do("AUTH", rc.password); err != nil {
						c.Close()
						return nil, err
					}
				}
				return
			},
		}

		rc.p = append(rc.p, p)
	}

}

// global redis pool
var redisPool RedisPool

func ClearAll(cacheKey string) error {
	cachedKeys, err := redis.Strings(redisPool.do("HKEYS", cacheKey))
	if err != nil {
		return err
	}
	for _, str := range cachedKeys {
		if _, err = redisPool.do("DEL", str); err != nil {
			return err
		}
	}
	_, err = redisPool.do("DEL", cacheKey)
	return err
}

func NewInit(config string) {
	err := redisPool.StartAndGC(config)
	if err != nil {
		log.Critical(err)
		panic(err)
	}
	return
}
