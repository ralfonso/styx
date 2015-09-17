# Styx
### POC for a Redis Cluster aware replication proxy

Styx is a proof of concept proxy for doing GET/SET and PFADD/PFCOUNT
commands against a Redis Cluster. A system such as this would be
useful in a multi-DC master/master setup or if a system has old
Redis clients that do not understand the Cluster semantics.

Styx operates on discrete Redis Clusters. One cluster is considered
"local" and has synchronous writes proxied to it. The other cluster
is "remote" and has writes asyncronously replayed.

Styx caches Redis Cluster slot mapping and smartly routes requests
if a keyslot is known.

Redis command parsing is handled by a very cool Redis protocol
implementation from the Docker team.

### Run it

Styx is run via docker and docker-compose.


To bring up the cluster with two Redis servers (local/remote, non Cluster)
```
docker-compose up
```

This will make Styx available to the host machine on port 6389

To test some commands (only GET/SET/PFADD/PFCOUNT are supported)

```
redis-cli -h docker-ip -p 6389
```

#### Unit Tests

Test coverage is far from comprehensive. The code is
currently not structured in a manner to ease
dependency injection / mocking.

```
make deps
make test
```

`make deps` requires the Glide vendoring tool

To run the tests in a docker container:

```
docker build -t styx .
docker run styx sh -c 'make deps && make test'
```
