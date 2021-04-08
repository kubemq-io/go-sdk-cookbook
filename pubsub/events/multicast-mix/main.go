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
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-multicast-mix"),
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
		ClientId: "go-sdk-cookbook-pubsub-events-multicast-mix-subscriber",
	}, func(msg *kubemq.Event, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver A - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
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
		ClientId:         "go-sdk-cookbook-pubsub-events-store-multicast-subscriber",
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
	queuesClient, err := kubemq.NewQueuesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
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

	done, err := queuesClient.Subscribe(ctx, &kubemq.ReceiveQueueMessagesRequest{
		ClientID:            "go-sdk-cookbook-pubsub-queues-multicast-subscriber",
		Channel:             "q1",
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     10,
	}, func(messages *kubemq.ReceiveQueueMessagesResponse, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			for _, msg := range messages.Messages {
				log.Printf("Queue Subscriber: MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
		}
	})

	time.Sleep(100 * time.Millisecond)
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	counter := 0
	for {
		counter++
		err = eventsClient.Send(ctx, kubemq.NewEvent().
			SetId("some-id").
			SetChannel("e1;events_store:es1;queues:q1").
			SetMetadata("some-metadata").
			SetBody([]byte(fmt.Sprintf("hello kubemq - sending event %d", counter))))

		if err != nil {
			log.Println(fmt.Sprintf("error sedning event store %d, error: %s", counter, err))
		}
		select {
		case <-gracefulShutdown:
			done <- struct{}{}
			break
		default:
			time.Sleep(time.Second)
		}
	}
}
