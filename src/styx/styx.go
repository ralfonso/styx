package main

import (
	"log"

	server "github.com/docker/go-redis-server"
)

func main() {

	localCluster := NewRedisCluster([]string{"192.168.59.103:7000"})
	remoteCluster := NewRedisCluster([]string{"192.168.59.103:7000"})
	styxCluster := StyxCluster{
		LocalCluster:  localCluster,
		RemoteCluster: remoteCluster,
	}

	handlerConfig := HandlerConfig{
		Cluster: styxCluster,
	}

	handler := NewHandler(handlerConfig)

	serverConfig := server.DefaultConfig()
	serverConfig.Handler(handler)

	server, err := server.NewServer(serverConfig)
	if err != nil {
		panic(err)
	}
	log.Print(server.ListenAndServe())
}
