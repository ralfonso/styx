package main

import (
	"log"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

// caches the ownership of a Slot in Redis cluster so we can make
// a best effort to send requests to the Redis host that owns the key
type SlotCache map[uint16]string

type RedisCluster struct {
	sync.RWMutex
	PoolSize int
	Pools    map[string]Pool
	Slots    SlotCache
}

// laaaaame. so we can mock redis.Pool
type Pool interface {
	Get() redis.Conn
	ActiveCount() int
	Close() error
}

func createRedisPool(endpoint string, redisTimeout time.Duration, redisIdleTimeout, redisMaxIdle, redisMaxActive int) Pool {
	timeout := time.Duration(redisTimeout) * time.Millisecond

	// TODO add a TestOnBorrow func that periodically checks the health of the connection
	return &redis.Pool{
		MaxIdle:     redisMaxIdle,
		MaxActive:   redisMaxActive,
		IdleTimeout: time.Duration(redisIdleTimeout) * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.DialTimeout("tcp", endpoint, timeout, timeout, timeout)
			if err != nil {
				return nil, err
			}
			return conn, err
		},
	}
}

func NewRedisCluster(hosts []string, poolSize int) *RedisCluster {
	pools := make(map[string]Pool)
	for _, host := range hosts {
		log.Printf("creating pool for %s", host)
		// XXX remove hardcoded timeout values
		pools[host] = createRedisPool(host, 500, 240, poolSize, poolSize)
	}
	return &RedisCluster{
		PoolSize: poolSize,
		Pools:    pools,
		Slots:    make(SlotCache),
	}
}

type StyxCluster struct {
	// these servers are used as the "local active".
	// operations from clients are synchronous
	LocalCluster *RedisCluster

	// these servers are remote/passive to this instance of Styx.
	// Operations from clients are replayed asynchronously
	// TODO support multiple remote clusters
	RemoteCluster *RedisCluster
}
