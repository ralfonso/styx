package main

import "log"

type Replayer struct {
	workerCount  int
	workers      []replayWorker
	client       ClusterClient
	cluster      *RedisCluster
	shutdownChan chan struct{}
	queue        chan RedisCommand
}

type replayWorker struct {
	queue        chan RedisCommand
	client       ClusterClient
	cluster      *RedisCluster
	shutdownChan chan struct{}
}

func NewReplayer(workerCount int, cluster *RedisCluster, client ClusterClient, replayQueue chan RedisCommand) *Replayer {
	shutdownChan := make(chan struct{}, 1)

	return &Replayer{
		workerCount:  workerCount,
		client:       client,
		cluster:      cluster,
		queue:        replayQueue,
		shutdownChan: shutdownChan,
	}
}

func (r *Replayer) Start() {
	workers := make([]replayWorker, r.workerCount)
	for i := 0; i < r.workerCount; i++ {
		worker := replayWorker{
			cluster:      r.cluster,
			client:       r.client,
			queue:        r.queue,
			shutdownChan: r.shutdownChan,
		}
		workers[i] = worker
		go worker.Exec()
	}

	r.workers = workers
}

func (w replayWorker) Exec() {
	// TODO add a healthiness factor so we don't allow a slow cluster to delay replay ops
	for {
		select {
		case <-w.shutdownChan:
			return
		case cmd := <-w.queue:
			key := cmd.args[0].(string)
			conn := connByKey(key, w.cluster)
			log.Printf("replaying op %s %s", cmd.cmd, cmd.args)
			w.client.redirectingDo(w.client.redirectionsAllowed, conn, w.cluster, cmd)
		}
	}
}
