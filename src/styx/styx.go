package main

import (
	"log"

	server "github.com/docker/go-redis-server"
)

func main() {

	handlerConfig := HandlerConfig{
		Server: "192.168.59.103:6379",
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
