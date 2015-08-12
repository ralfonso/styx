package main

import (
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
	"github.com/howeyc/crc16"
)

const (
	redisSlotMax = 16384
	maxInt       = int((^uint(0)) >> 1)
)

type HandlerConfig struct {
	Cluster StyxCluster
}

type Handler struct {
	Cluster StyxCluster
}

func NewHandler(config HandlerConfig) *Handler {
	return &Handler{Cluster: config.Cluster}
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
	return crc16.ChecksumCCITT([]byte(key)) % redisSlotMax
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

func (h *Handler) clusterDo(conn redis.Conn, cmd string, args ...interface{}) (interface{}, error) {
	v, err := conn.Do(cmd, args...)
	if err != nil {
		if err.Error()[0:3] == "ASK" {
			clusterKeyError, err := parseClusterKeyError(err.Error())
			if err != nil {
				return nil, err
			}

			h.Cluster.LocalCluster.Lock()
			if pool, ok := h.Cluster.LocalCluster.Pools[clusterKeyError.Host]; ok {
				conn.Close()
				conn = pool.Get()
				h.Cluster.LocalCluster.Unlock()
				return h.clusterDo(conn, cmd, args...)
			} else {
				conn.Close()
				// create the pool
				log.Printf("Creating pool for %s", clusterKeyError.Host)
				pool := createRedisPool(clusterKeyError.Host, 500, 240, 10, 10)
				h.Cluster.LocalCluster.Pools[clusterKeyError.Host] = pool
				h.Cluster.LocalCluster.Unlock()
				conn = pool.Get()
				return h.clusterDo(conn, cmd, args...)
			}

			h.Cluster.LocalCluster.Unlock()
		} else if err.Error()[0:5] == "MOVED" {
			clusterKeyError, err := parseClusterKeyError(err.Error())
			if err != nil {
				return nil, err
			}

			h.Cluster.LocalCluster.Lock()
			log.Printf("Caching slot %d to host %s", clusterKeyError.Slot, clusterKeyError.Host)
			h.Cluster.LocalCluster.Slots[clusterKeyError.Slot] = clusterKeyError.Host

			if pool, ok := h.Cluster.LocalCluster.Pools[clusterKeyError.Host]; ok {
				conn.Close()
				conn = pool.Get()
				h.Cluster.LocalCluster.Unlock()
				return h.clusterDo(conn, cmd, args...)
			} else {
				conn.Close()
				// create the pool
				log.Printf("Creating pool for %s", clusterKeyError.Host)
				pool := createRedisPool(clusterKeyError.Host, 500, 240, 10, 10)
				h.Cluster.LocalCluster.Pools[clusterKeyError.Host] = pool
				h.Cluster.LocalCluster.Unlock()
				conn = pool.Get()
				return h.clusterDo(conn, cmd, args...)
			}
		}
	}

	return v, err
}

func (h *Handler) Pfadd(key string, values ...[]byte) (int, error) {
	conn := h.connByKey(key, h.Cluster.LocalCluster.Pools, h.Cluster.LocalCluster.Slots)
	defer conn.Close()

	stringValues := make([]interface{}, len(values)+1)
	stringValues[0] = key
	for i := 1; i < len(stringValues); i++ {
		stringValues[i] = string(values[i-1])
	}

	v, err := h.clusterDo(conn, "pfadd", stringValues...)
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
	defer conn.Close()

	v, err := conn.Do("pfcount", key)
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
