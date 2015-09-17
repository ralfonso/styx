package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func NewMockRedisPool() *MockRedisPool {
	pool := &MockRedisPool{}
	return pool
}

func TestChooseFewestConns(t *testing.T) {

	pools := make(map[string]Pool)
	mockPool := NewMockRedisPool()
	mockPool.Active = 10
	pools["host1:6379"] = mockPool
	assert.Equal(t, mockPool, chooseFewestConns(pools), "chose the correct pool")

	mockPool2 := NewMockRedisPool()
	mockPool2.Active = 1
	pools["host2:6379"] = mockPool2
	mockPool3 := NewMockRedisPool()
	mockPool3.Active = 0
	pools["host3:6379"] = mockPool3
	assert.Equal(t, mockPool3, chooseFewestConns(pools), "chose the correct pool")
}

func TestParseRedirection(t *testing.T) {
	errType := "MOVED"
	slot := uint16(123)
	host := "server:6379"
	redir := fmt.Sprintf("%s %d %s", errType, slot, host)
	redirErr, err := _parseRedirection(redir)
	assert.Nil(t, err)
	assert.Equal(t, movedError, redirErr.errType, "redir errType matches")
	assert.Equal(t, slot, redirErr.slot, "redir slot matches")
	assert.Equal(t, host, redirErr.host, "redir host matches")
}

func TestParseIfRedir(t *testing.T) {
	errType := "MOVED"
	slot := uint16(123)
	host := "server:6379"
	redir := errors.New(fmt.Sprintf("%s %d %s", errType, slot, host))
	redirErr, isRedir := parseIfRedir(redir)
	assert.IsType(t, redirectionError{}, redirErr)
	assert.True(t, isRedir)

	nonRedir := errors.New("TOFU 123")
	redirErr, isRedir = parseIfRedir(nonRedir)
	assert.IsType(t, redirectionError{}, redirErr)
	assert.Zero(t, redirErr.errType, "empty type returned")
	assert.Zero(t, redirErr.slot, "empty slot returned")
	assert.False(t, isRedir)
}

type MockConn struct {
	mock.Mock
	Mutex sync.Mutex
}

func (m *MockConn) Do(cmd string, cmdArgs ...interface{}) (interface{}, error) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	var args mock.Arguments
	if len(cmdArgs) == 0 {
		args = m.Mock.Called(cmd)
	} else {
		args = m.Mock.Called(cmd, cmdArgs)
	}
	return args.Get(0), args.Error(1)
}

func (m *MockConn) Send(cmd string, cmdArgs ...interface{}) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if len(cmdArgs) == 0 {
		return m.Mock.Called(cmd).Error(0)
	}
	return m.Mock.Called(cmd, cmdArgs).Error(0)
}

func (m *MockConn) Receive() (interface{}, error) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	args := m.Mock.Called()
	return args.Get(0), args.Error(1)
}

func (m *MockConn) Flush() error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.Mock.Called().Error(0)
}

func (m *MockConn) Err() error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.Mock.Called().Error(0)
}

func (m *MockConn) Close() error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.Mock.Called().Error(0)
}

type MockRedisPool struct {
	mock.Mock
	Conn *MockConn

	Active int
}

func (m *MockRedisPool) Get() redis.Conn {
	return m.Conn
}

func (m *MockRedisPool) ActiveCount() int {
	return m.Active
}

func (m *MockRedisPool) Close() error {
	return m.Mock.Called().Error(0)
}
