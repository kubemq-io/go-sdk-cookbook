package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-offset-sender"),
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
	randomChannel := uuid.New().String()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		result, err := eventsStoreClient.Send(ctx, kubemq.NewEventStore().
			SetChannel(randomChannel).
			SetMetadata("some-metadata").
			SetTags(map[string]string{"key1": "value1", "key2": "value2"}).
			SetBody([]byte(fmt.Sprintf("hello kubemq - sending event store %d", i))))

		if err != nil {
			log.Println(fmt.Sprintf("error sedning event %d, error: %s", i, err))
		}
		log.Printf("Send Message %d ,Result: Id: %s, Sent: %t\n", i, result.Id, result.Sent)
	}
	endTime := time.Now()

	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          randomChannel,
		ClientId:         "go-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-first",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver Start From First- Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          randomChannel,
		ClientId:         "go-sdk-cookbook-pubsub-events-store-offset-receiver-start-seq",
		SubscriptionType: kubemq.StartFromSequence(5),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver Start From Seq- Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          randomChannel,
		ClientId:         "go-sdk-cookbook-pubsub-events-store-offset-receiver-start-time-delta",
		SubscriptionType: kubemq.StartFromTimeDelta(3 * time.Second),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver Start From Time Delta- Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          randomChannel,
		ClientId:         "go-sdk-cookbook-pubsub-events-store-offset-receiver-start-time",
		SubscriptionType: kubemq.StartFromTime(endTime.Add(-3 * time.Second)),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver Start From Time - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
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
	for {

		select {
		case <-gracefulShutdown:
			break
		default:
			time.Sleep(time.Second)
		}
	}
}
