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
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-stream-sender"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := sender.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	receiverA, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-stream-receiver-a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := receiverA.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	receiverB, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-stream-receiver-b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := receiverB.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	channel := "events"
	go func() {
		errCh := make(chan error)
		eventsCh, err := receiverA.SubscribeToEvents(ctx, channel, "group1", errCh)
		if err != nil {
			log.Fatal(err)
			return

		}
		for {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					log.Println("Receiver A - Event Received, done")
					return
				}
				log.Printf("Receiver A - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		errCh := make(chan error)
		eventsCh, err := receiverB.SubscribeToEvents(ctx, channel, "group1", errCh)
		if err != nil {
			log.Fatal(err)
			return
		}
		for {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					log.Println("Receiver B - Event Received, done")
					return
				}
				log.Printf("Receiver B - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
			case <-ctx.Done():
				return
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	eventsCh := make(chan *kubemq.Event, 1)
	errCh := make(chan error, 1)
	go sender.StreamEvents(ctx, eventsCh, errCh)
	counter := 0
	for {
		select {
		case <-time.After(time.Second):
			counter++
			eventsCh <- sender.E().
				SetId("some-id").
				SetChannel(channel).
				SetMetadata("some-metadata").
				SetBody([]byte(fmt.Sprintf("hello kubemq - sending event stream %d", counter)))
		case err := <-errCh:
			log.Fatal(err)
		case <-gracefulShutdown:
			return
		}
	}
}
