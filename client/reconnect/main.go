package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

// Basic client with re-connect connectivity - good for subscriber side

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-client-reconnect"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true),
		kubemq.WithReconnectInterval(time.Second),
		// value = 0 , reconnect indefinitely
		kubemq.WithMaxReconnects(0))
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
