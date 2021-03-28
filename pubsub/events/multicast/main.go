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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-multicast-sender"),
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

	eventsStoreReceiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-multicast-receiver-a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreReceiver.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	eventsReceiverB, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-multicast-receiver-b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsReceiverB.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		errCh := make(chan error)
		eventsCh, err := eventsStoreReceiver.SubscribeToEvents(ctx, "events.a", "", errCh)
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
		eventsCh, err := eventsReceiverB.SubscribeToEvents(ctx, "events.b", "", errCh)
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
	multicastChannel := "events.a;events.b"
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	counter := 0
	for {
		counter++
		err = sender.E().
			SetId("some-id").
			SetChannel(multicastChannel).
			SetMetadata("some-metadata").
			SetBody([]byte(fmt.Sprintf("hello kubemq - sending event %d", counter))).
			Send(ctx)
		if err != nil {
			log.Println(fmt.Sprintf("error sedning event %d, error: %s", counter, err))

		}
		select {
		case <-gracefulShutdown:
			break
		default:
			time.Sleep(time.Second)
		}
	}

}
