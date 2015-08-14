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

const (
	redisSlotMax uint16 = 16384
	maxInt              = int((^uint(0)) >> 1)
)

type HandlerConfig struct {
	Cluster                StyxCluster
	ReplicationWorkerCount int
}

type ReplayOp struct {
	handler *Handler
	cmd     string
	args    []interface{}
}

type Handler struct {
	Cluster            StyxCluster
	replicationWorkers ReplicationWorkers
	replicationQueue   chan ReplayOp
}

type ReplicationWorkers struct {
	Cluster      *RedisCluster
	ShutdownChan chan struct{}
}

func NewReplicationWorkerPool(poolSize int, cluster *RedisCluster, rQueue chan ReplayOp) ReplicationWorkers {
	shutdownChan := make(chan struct{}, 1)

	for i := 0; i < poolSize; i++ {
		go func() {
			select {
			case <-shutdownChan:
				return
			case replayOp := <-rQueue:
				key := replayOp.args[0].(string)
				conn := replayOp.handler.connByKey(key, cluster.Pools, cluster.Slots)
				log.Printf("replaying op to %s", conn)
				replayOp.handler.clusterDo(true, 5, conn, replayOp.cmd, replayOp.args...)
			}
		}()
	}
	return ReplicationWorkers{
		Cluster:      cluster,
		ShutdownChan: shutdownChan,
	}
}

func NewHandler(config HandlerConfig) *Handler {
	rQueue := make(chan ReplayOp, config.ReplicationWorkerCount)

	return &Handler{
		Cluster:            config.Cluster,
		replicationWorkers: NewReplicationWorkerPool(config.ReplicationWorkerCount, &config.Cluster.RemoteCluster, rQueue),
		replicationQueue:   rQueue,
	}
}

func chooseLeastConns(pools map[string]*redis.Pool) *redis.Pool {
	leastActive := maxInt
	var leastActivePool *redis.Pool = nil

	for _, pool := range pools {
		active := pool.ActiveCount()
		if active < leastActive {
			leastActive = active
			leastActivePool = pool
		}
	}

	return leastActivePool
}

func keySlot(key string) uint16 {
	cs := crc16.Crc16([]byte(key))
	return cs % redisSlotMax
}

func (h *Handler) connByKey(key string, pools map[string]*redis.Pool, slotCache SlotCache) redis.Conn {
	slot := keySlot(key)
	if owner, ok := slotCache[slot]; ok {
		return pools[owner].Get()
	}

	pool := chooseLeastConns(pools)
	return pool.Get()
}

type ClusterKeyError struct {
	Action string
	Slot   uint16
	Host   string
}

func parseClusterKeyError(errStr string) (*ClusterKeyError, error) {
	parts := strings.Split(errStr, " ")
	slot, err := strconv.ParseInt(parts[1], 10, 16)
	if err != nil {
		return nil, err
	}
	return &ClusterKeyError{
		Action: parts[0],
		Slot:   uint16(slot),
		Host:   parts[2],
	}, nil
}

func (h *Handler) queueOpForReplication(cmd string, args ...interface{}) {
	replayOp := ReplayOp{
		handler: h,
		cmd:     cmd,
		args:    args,
	}
	// XXX this could block which is why this func is run in a goroutine.
	// TODO add expiration for replay operations so we don't accumulate forever in case the remote end is lost
	h.replicationQueue <- replayOp
}

func (h *Handler) clusterDo(isReplay bool, redirectsAllowed int, conn redis.Conn, cmd string, args ...interface{}) (interface{}, error) {
	v, err := conn.Do(cmd, args...)
	if err != nil {
		if err.Error()[0:3] == "ASK" {
			if redirectsAllowed == 0 {
				return nil, errors.New(fmt.Sprintf("exceeded maximum number of Redis cluster redirects: %s", err.Error()))
			}

			redirectsAllowed -= 1
			clusterKeyError, err := parseClusterKeyError(err.Error())
			if err != nil {
				return nil, err
			}

			h.Cluster.LocalCluster.Lock()
			if pool, ok := h.Cluster.LocalCluster.Pools[clusterKeyError.Host]; ok {
				conn.Close()
				conn = pool.Get()
				defer conn.Close()
				h.Cluster.LocalCluster.Unlock()
				return h.clusterDo(isReplay, redirectsAllowed, conn, cmd, args...)
			} else {
				conn.Close()
				// create the pool
				log.Printf("Creating pool for %s", clusterKeyError.Host)
				pool := createRedisPool(clusterKeyError.Host, 500, 240, 10, 10)
				h.Cluster.LocalCluster.Pools[clusterKeyError.Host] = pool
				h.Cluster.LocalCluster.Unlock()
				conn = pool.Get()
				defer conn.Close()
				return h.clusterDo(isReplay, redirectsAllowed, conn, cmd, args...)
			}

			h.Cluster.LocalCluster.Unlock()
		} else if err.Error()[0:5] == "MOVED" {
			if redirectsAllowed == 0 {
				return nil, errors.New(fmt.Sprintf("exceeded maximum number of Redis cluster redirects: %s", err.Error()))
			}

			redirectsAllowed -= 1

			clusterKeyError, err := parseClusterKeyError(err.Error())
			if err != nil {
				return nil, err
			}

			h.Cluster.LocalCluster.Lock()
			h.Cluster.LocalCluster.Slots[clusterKeyError.Slot] = clusterKeyError.Host

			if pool, ok := h.Cluster.LocalCluster.Pools[clusterKeyError.Host]; ok {
				conn.Close()
				conn = pool.Get()
				defer conn.Close()
				h.Cluster.LocalCluster.Unlock()
				return h.clusterDo(isReplay, redirectsAllowed, conn, cmd, args...)
			} else {
				conn.Close()
				// create the pool
				log.Printf("Creating pool for %s", clusterKeyError.Host)
				pool := createRedisPool(clusterKeyError.Host, 500, 240, 10, 10)
				h.Cluster.LocalCluster.Pools[clusterKeyError.Host] = pool
				h.Cluster.LocalCluster.Unlock()
				conn = pool.Get()
				defer conn.Close()
				return h.clusterDo(isReplay, redirectsAllowed, conn, cmd, args...)
			}
		}
	}

	if !isReplay {
		go h.queueOpForReplication(cmd, args...)
	}
	return v, err
}

func (h *Handler) Pfadd(key string, values ...[]byte) (int, error) {
	conn := h.connByKey(key, h.Cluster.LocalCluster.Pools, h.Cluster.LocalCluster.Slots)

	stringValues := make([]interface{}, len(values)+1)
	stringValues[0] = key
	for i := 1; i < len(stringValues); i++ {
		stringValues[i] = string(values[i-1])
	}

	v, err := h.clusterDo(false, 5, conn, "pfadd", stringValues...)
	conn.Close()
	if err != nil {
		return 0, err
	}

	rv, conversionError := redis.Int(v, err)
	if conversionError != nil {
		return 0, errors.New("non-integer reply received for PFADD")
	}

	return rv, nil
}

func (h *Handler) Pfcount(key string) (int, error) {
	conn := h.connByKey(key, h.Cluster.LocalCluster.Pools, h.Cluster.LocalCluster.Slots)

	v, err := h.clusterDo(false, 5, conn, "pfcount", stringValues...)
	conn.Close()
	if err != nil {
		return 0, err
	}

	rv, conversionError := redis.Int(v, err)
	if conversionError != nil {
		return 0, errors.New("non-integer reply received for PFADD")
	}

	return rv, nil
}

func (h *Handler) Get(key string) ([]byte, error) {
	conn := h.connByKey(key, h.Cluster.LocalCluster.Pools, h.Cluster.LocalCluster.Slots)
	defer conn.Close()

	v, err := conn.Do("get", key)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	rv, conversionError := redis.Bytes(v, err)
	if conversionError != nil {
		return nil, errors.New("non-bulkstring reply received for GET")
	}

	return rv, nil
}

func (h *Handler) Set(key string, value []byte) error {
	conn := h.connByKey(key, h.Cluster.LocalCluster.Pools, h.Cluster.LocalCluster.Slots)
	defer conn.Close()

	_, err := conn.Do("set", key, value)
	return err
}
