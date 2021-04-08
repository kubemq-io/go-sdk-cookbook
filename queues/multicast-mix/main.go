package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := kubemq.NewQueuesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-multicast-mix"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-events-multicast-mix"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	err = eventsClient.Subscribe(ctx, &kubemq.EventsSubscription{
		Channel:  "e1",
		ClientId: "go-sdk-cookbook-queues-events-multicast-mix",
	}, func(msg *kubemq.Event, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Events Receiver  - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})

	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
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

	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          "es1",
		ClientId:         "go-sdk-cookbook-queues-events-store-multicast-mix",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
			return
		}
		log.Printf("Events Store Receiver - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
	})
	if err != nil {
		log.Fatal(err)
		return

	}
	done, err := queuesClient.Subscribe(ctx, &kubemq.ReceiveQueueMessagesRequest{
		ClientID:            "go-sdk-cookbook-queues-multicast-mix",
		Channel:             "q1",
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	}, func(response *kubemq.ReceiveQueueMessagesResponse, err error) {
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Pull Received %d Messages\n", response.MessagesReceived)
	})
	if err != nil {
		log.Fatal(err)
	}
	var batch []*kubemq.QueueMessage
	for i := 0; i < 10; i++ {
		batch = append(batch, kubemq.NewQueueMessage().
			SetChannel("q1;events:e1;events_store:es1").
			SetBody([]byte(fmt.Sprintf("Batch Message %d", i))))

	}
	_, err = queuesClient.Batch(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	select {

	case <-gracefulShutdown:

	}
	done <- struct{}{}

}
