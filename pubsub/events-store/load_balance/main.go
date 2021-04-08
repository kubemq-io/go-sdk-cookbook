package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-load-balance"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	channel := "events-store.loadbalance"
	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          channel,
		Group:            "g1",
		ClientId:         "go-sdk-cookbook-pubsub-events-store-load-balance-subscriber-A",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver A - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          channel,
		Group:            "g1",
		ClientId:         "go-sdk-cookbook-pubsub-events-store-load-balance-subscriber-B",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver B - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	streamSender, err := eventsStoreClient.Stream(ctx, func(result *kubemq.EventStoreResult, err error) {
		if err != nil {
			log.Fatal()
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	counter := 0
	for {
		select {
		case <-time.After(time.Second):
			counter++
			err := streamSender(kubemq.NewEventStore().
				SetId("some-id").
				SetChannel(channel).
				SetMetadata("some-metadata").
				SetBody([]byte(fmt.Sprintf("hello kubemq - sending event stream %d", counter))))
			if err != nil {
				log.Fatal(err)
			}
		case <-gracefulShutdown:
			return
		}
	}

}
