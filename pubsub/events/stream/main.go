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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-stream"),
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
	channel := "events.stream"
	err = eventsClient.Subscribe(ctx, &kubemq.EventsSubscription{
		Channel:  channel,
		ClientId: "go-sdk-cookbook-pubsub-events-stream-subscriber",
	}, func(msg *kubemq.Event, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	})

	time.Sleep(100 * time.Millisecond)
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	streamSender, err := eventsClient.Stream(ctx, func(err error) {
		if err != nil {
			log.Fatal(err)
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
			err := streamSender(kubemq.NewEvent().
				SetId("some-id").
				SetChannel(channel).
				SetMetadata("some-metadata").
				AddTag("key", "value").
				SetBody([]byte(fmt.Sprintf("hello kubemq - sending event stream %d", counter))))

			if err != nil {
				log.Fatal(err)
			}
		case <-gracefulShutdown:
			return
		}
	}
}
