package main

import (
	"errors"

	"github.com/garyburd/redigo/redis"
)

type HandlerConfig struct {
	client ClusterClient
}

type Handler struct {
	client ClusterClient
}

func NewHandler(config HandlerConfig) *Handler {
	return &Handler{
		client: config.client,
	}
}

func (h *Handler) Pfadd(key string, values ...[]byte) (int, error) {
	stringValues := make([]interface{}, len(values)+1)
	stringValues[0] = key
	for i := 1; i < len(stringValues); i++ {
		stringValues[i] = string(values[i-1])
	}

	v, err := h.client.Do("pfadd", stringValues...)
	if err != nil {
		return 0, err
	}

	rv, conversionError := redis.Int(v, err)
	if conversionError != nil {
		return 0, errors.New("non-integer reply received for PFADD")
	}

	return rv, nil
}

func (h *Handler) Pfcount(key string, values ...[]byte) (int, error) {
	stringValues := make([]interface{}, len(values)+1)
	stringValues[0] = key
	for i := 1; i < len(stringValues); i++ {
		stringValues[i] = string(values[i-1])
	}
	v, err := h.client.Do("pfcount", stringValues...)
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
	v, err := h.client.Do("get", key)
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
	_, err := h.client.Do("set", key, value)
	return err
}
