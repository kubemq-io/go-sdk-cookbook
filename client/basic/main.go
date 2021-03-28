package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

// Basic Client with ping
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-client-basic"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	result, err := client.Ping(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%++v", result)
}
