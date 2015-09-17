package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
	crc16 "github.com/joaojeronimo/go-crc16"
)

type redirErrorType int

const (
	redisSlotMax  uint16         = 16384
	maxInt                       = int((^uint(0)) >> 1)
	askErrorStr   string         = "ASK"
	movedErrorStr string         = "MOVED"
	askError      redirErrorType = iota
	movedError    redirErrorType = iota
)

type ClusterClient struct {
	// The "local" cluster. Writes are synchronous
	L *RedisCluster

	// The "remote" cluster. Writes to the local cluster are replayed asyncronously here
	R *RedisCluster

	redirectionsAllowed int

	replayWorkerCount int
	replayQueue       chan RedisCommand
}

type RedisCommand struct {
	cmd  string
	args []interface{}
}

type ReplicationWorkers struct {
	Cluster      *RedisCluster
	ShutdownChan chan struct{}
}

type redirectionError struct {
	errType redirErrorType
	slot    uint16
	host    string
}

// a map of our supported write commands.
// used to determine if a command should be enqueue for replay
var writeCommands = map[string]bool{
	"PFADD": true,
	"SET":   true,
}

func NewClusterClient(cluster StyxCluster, replayWorkerCount, redirectsAllowed int, replayQueue chan RedisCommand) ClusterClient {
	return ClusterClient{
		L:                   cluster.LocalCluster,
		R:                   cluster.RemoteCluster,
		redirectionsAllowed: redirectsAllowed,
		replayWorkerCount:   replayWorkerCount,
		// TODO determine an appropriate size for the queue
		replayQueue: replayQueue,
	}
}

// suuuuuuper basic load balancing. requires a read lock :|
func chooseFewestConns(pools map[string]Pool) Pool {
	leastActive := maxInt
	var leastActivePool Pool = nil

	for _, pool := range pools {
		active := pool.ActiveCount()
		if active < leastActive {
			leastActive = active
			leastActivePool = pool
		}
	}

	return leastActivePool
}

// redis cluster uses CRC16 + modulo to determine a key's slot
func keySlot(key string) uint16 {
	cs := crc16.Crc16([]byte(key))
	return cs % redisSlotMax
}

// takes a Redis key and makes an effort to return a redis connection
// to either the server that owns the key/slot, or the server
// with the fewest active connections (which may result in a redir)
func connByKey(key string, cluster *RedisCluster) redis.Conn {
	slot := keySlot(key)

	var pool Pool

	cluster.RLock()
	if owner, ok := cluster.Slots[slot]; ok {
		pool = cluster.Pools[owner]
	} else {
		pool = chooseFewestConns(cluster.Pools)
	}
	cluster.RUnlock()

	return pool.Get()
}

func isWriteCommand(cmd string) bool {
	cmd = strings.ToUpper(cmd)
	_, ok := writeCommands[cmd]
	return ok
}

// proxies a redis command to the local cluster and determines
// eligibility for replay. enqueues if eligible
func (c *ClusterClient) Do(cmd string, args ...interface{}) (interface{}, error) {
	var key string
	if len(args) > 0 {
		key, _ = args[0].(string)
	}

	if key == "" {
		return nil, errors.New("cannot determine command destination without a key")
	}

	rCmd := RedisCommand{
		cmd:  cmd,
		args: args,
	}

	// this method is only used by the external handler, which means we always want to
	// determine the _local_ shard
	conn := connByKey(key, c.L)
	v, err := c.redirectingDo(c.redirectionsAllowed, conn, c.L, rCmd)
	if err == nil && isWriteCommand(rCmd.cmd) {
		go c.queueOpForReplication(rCmd)
	}

	return v, err
}

// handle a redirection response from redis
func _parseRedirection(errStr string) (redirErr redirectionError, err error) {
	parts := strings.Split(errStr, " ")
	slot, err := strconv.ParseInt(parts[1], 10, 16)
	if err != nil {
		log.Printf("parse redir error: %s", err)
		return redirErr, err
	}

	var rErrType redirErrorType

	if parts[0] == askErrorStr {
		rErrType = askError
	} else if parts[0] == movedErrorStr {
		rErrType = movedError
	}

	return redirectionError{
		errType: rErrType,
		slot:    uint16(slot),
		host:    parts[2],
	}, nil
}

// checks if a response is a redirection and hands the errstr
// off to be parsed
func parseIfRedir(err error) (redirErr redirectionError, isRedir bool) {
	errStr := err.Error()
	isRedir = errStr[0:3] == askErrorStr || errStr[0:5] == movedErrorStr
	if isRedir {
		var parseError error
		redirErr, parseError = _parseRedirection(err.Error())
		if parseError != nil {
			return redirErr, false
		}
	}

	return redirErr, isRedir
}

// recursive execution method that decrements on every attempt. Tries to handle a number of
// redis redirects without getting stuck in a loop
func (c *ClusterClient) redirectingDo(redirectsAllowed int, conn redis.Conn, cluster *RedisCluster, cmd RedisCommand) (interface{}, error) {
	v, err := conn.Do(cmd.cmd, cmd.args...)
	if err != nil {
		redirError, ok := parseIfRedir(err)
		if ok {
			conn.Close()

			if redirectsAllowed <= 0 {
				return nil, errors.New(fmt.Sprintf("exceeded maximum number of Redis cluster redirects: %s", err.Error()))
			}
			redirectsAllowed -= 1
			return c.handleRedirection(redirectsAllowed, redirError, cluster, cmd)
		}
	}

	conn.Close()
	return v, err
}

// On a redirect response, determines where the next attempt should go
func (c *ClusterClient) handleRedirection(redirectsAllowed int, redirError redirectionError, cluster *RedisCluster, cmd RedisCommand) (interface{}, error) {
	// XXX improve the delicate locking in this fn

	// we only cache the slot location for a MOVED
	if redirError.errType == movedError {
		cluster.Lock()
		cluster.Slots[redirError.slot] = redirError.host
		cluster.Unlock()
	}

	cluster.RLock()
	if pool, ok := cluster.Pools[redirError.host]; ok {
		// we have a connection pool for this host
		conn := pool.Get()
		cluster.RUnlock()
		return c.redirectingDo(redirectsAllowed, conn, cluster, cmd)
	} else {
		// we need to create a new pool
		cluster.Lock()
		log.Printf("Creating pool for %s", redirError.host)
		pool := createRedisPool(redirError.host, 500, 240, cluster.PoolSize, cluster.PoolSize)
		cluster.Pools[redirError.host] = pool
		cluster.Unlock()
		conn := pool.Get()
		return c.redirectingDo(redirectsAllowed, conn, cluster, cmd)
	}

	cluster.Unlock()
	return nil, errors.New("unknown Redis Cluster redirection error")
}

func (c *ClusterClient) queueOpForReplication(cmd RedisCommand) {
	// XXX this will block if the channel is full so we need to be in a goroutine
	// TODO add expiration for replay operations so we don't accumulate forever in case the remote end is lost
	c.replayQueue <- cmd
}
