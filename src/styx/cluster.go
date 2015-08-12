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
	Pools map[string]*redis.Pool
	Slots SlotCache
}

func createRedisPool(endpoint string, redisTimeout time.Duration, redisIdleTimeout, redisMaxIdle, redisMaxActive int) *redis.Pool {
	timeout := time.Duration(redisTimeout) * time.Millisecond

	return &redis.Pool{
		MaxIdle:     redisMaxIdle,
		MaxActive:   redisMaxActive,
		IdleTimeout: time.Duration(redisIdleTimeout) * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.DialTimeout("tcp", endpoint, timeout, timeout, timeout)
			if err != nil {
				log.Print(err)
				return nil, err
			}
			return conn, err
		},
	}
}

func NewRedisCluster(hosts []string) RedisCluster {
	pools := make(map[string]*redis.Pool)
	for _, host := range hosts {
		log.Printf("creating pool for %s", host)
		pools[host] = createRedisPool(host, 500, 240, 10, 10)
	}
	return RedisCluster{
		Pools: pools,
		Slots: make(SlotCache),
	}
}

type StyxCluster struct {
	// these servers are used as the "local active".
	// operations from clients are synchronous
	LocalCluster RedisCluster

	// these servers are remote/passive to this instance of Styx.
	// Operations from clients are replayed asynchronously
	RemoteCluster RedisCluster
}