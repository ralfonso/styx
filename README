# Styx
### POC for a Redis Cluster aware replication proxy

Styx is a proof of concept proxy for doing GET/SET and PFADD/PFCOUNT
commands against a Redis Cluster. A system such as this would be
useful in a multi-DC master/master setup or if a stack has old
Redis clients that do not understand the Cluster semantics.

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

#### Unit Tests (limited)

```
make deps
make test
```

`make deps` requires the Glide vendoring tool

To run the tests in a docker container:

```
docker build -t styx .
docker run styx cd /go/src/styx && make deps && make test
```
