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

	workerCount := 30
	redirectionLimit := 5

	// TODO determine a proper queue size
	replayQueue := make(chan RedisCommand, workerCount*100)
	clusterClient := NewClusterClient(styxCluster, workerCount, redirectionLimit, replayQueue)

	handlerConfig := HandlerConfig{
		client: clusterClient,
	}

	handler := NewHandler(handlerConfig)
	replayer := NewReplayer(workerCount, remoteCluster, clusterClient, replayQueue)
	replayer.Start()

	serverConfig := server.DefaultConfig().Host("0.0.0.0").Handler(handler)

	server, err := server.NewServer(serverConfig)
	if err != nil {
		panic(err)
	}
	log.Print(server.ListenAndServe())
}
