package main

import (
	"log"

	server "github.com/docker/go-redis-server"
)

func main() {

	localCluster := NewRedisCluster([]string{"server1:7000"})
	remoteCluster := NewRedisCluster([]string{"server2:7000"})
	styxCluster := StyxCluster{
		LocalCluster:  localCluster,
		RemoteCluster: remoteCluster,
	}

	handlerConfig := HandlerConfig{
		Cluster:                styxCluster,
		ReplicationWorkerCount: 10,
	}

	handler := NewHandler(handlerConfig)

	serverConfig := server.DefaultConfig().Host("0.0.0.0").Handler(handler)

	server, err := server.NewServer(serverConfig)
	if err != nil {
		panic(err)
	}
	log.Print(server.ListenAndServe())
}
