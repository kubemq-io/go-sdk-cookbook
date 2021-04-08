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
	channel := "events-store.single"
	err = eventsStoreClient.Subscribe(ctx, &kubemq.EventsStoreSubscription{
		Channel:          channel,
		ClientId:         "go-sdk-cookbook-pubsub-events-store-single-subscriber",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}, func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
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
	counter := 0
	for {
		counter++
		result, err := eventsStoreClient.Send(ctx, kubemq.NewEventStore().
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetTags(map[string]string{"key1": "value1", "key2": "value2"}).
			SetBody([]byte(fmt.Sprintf("hello kubemq - sending event %d", counter))))
		if err != nil {
			log.Println(fmt.Sprintf("error sedning event %d, error: %s", counter, err))

		}
		log.Printf("Send Result: Id: %s, Sent: %t\n", result.Id, result.Sent)
		select {
		case <-gracefulShutdown:
			break
		default:
			time.Sleep(time.Second)
		}
	}

}
