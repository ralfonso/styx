package main

import (
	"log"

	server "github.com/ralfonso/go-redis-server"
)

func main() {
	// TODO determine a proper replay worker count
	workerCount := 30
	localCluster := NewRedisCluster([]string{"redis1:6379"}, 500)

	// TODO support multiple remote clusters
	remoteCluster := NewRedisCluster([]string{"redis2:6379"}, workerCount)
	styxCluster := StyxCluster{
		LocalCluster:  localCluster,
		RemoteCluster: remoteCluster,
	}

	// TODO determine a sane Redis cluster redirection limit
	redirectionLimit := 5

	// TODO determine a proper queue size
	replayQueue := make(chan RedisCommand, workerCount*100)

	/* this is the client that handles proxying synchronous requests to the local redis
	cluster writes are acknowledged immediately after confirmation by the node owning
	the key in the local cluster. The client caches slot assignments as recommended
	by the Redis Cluster spec. Successful writes are enqueued for eventual consumption
	by the Replayer */
	clusterClient := NewClusterClient(styxCluster, workerCount, redirectionLimit, replayQueue)

	handlerConfig := HandlerConfig{
		client: clusterClient,
	}

	handler := NewHandler(handlerConfig)

	/* the replayer is the component that reads from a FIFO queue (channel) and
	sends commands to a remote Redis cluster (via the ClusterClient) */
	replayer := NewReplayer(workerCount, remoteCluster, clusterClient, replayQueue)
	replayer.Start()

	// set up a server using go-redis-server. Listening on port 6389
	serverConfig := server.DefaultConfig().Host("0.0.0.0").Handler(handler)

	server, err := server.NewServer(serverConfig)
	if err != nil {
		panic(err)
	}

	log.Print(server.ListenAndServe())
}
