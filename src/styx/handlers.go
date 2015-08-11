package main

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

type HandlerConfig struct {
	Server string
}

type Handler struct {
	Pool *redis.Pool
}

func NewHandler(config HandlerConfig) *Handler {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", config.Server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return &Handler{Pool: pool}
}

func (h *Handler) Pfadd(key string, values ...[]byte) (int, error) {
	conn := h.Pool.Get()
	defer conn.Close()

	stringValues := make([]interface{}, len(values)+1)
	stringValues[0] = key
	for i := 1; i < len(stringValues); i++ {
		stringValues[i] = string(values[i-1])
	}

	v, err := conn.Do("pfadd", stringValues...)
	if rv, ok := v.(int64); ok {
		return int(rv), err
	}

	return 0, errors.New("non-integer reply received for PFADD")
}
