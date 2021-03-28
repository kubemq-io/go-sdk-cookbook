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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-store-single-sender"),
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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-store-single-receiver-a"),
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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-store-single-receiver-b"),
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

	channel := "events-store"
	go func() {
		errCh := make(chan error)
		eventsCh, err := receiverA.SubscribeToEventsStore(ctx, channel, "", errCh, kubemq.StartFromFirstEvent())
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
					log.Println("Receiver A - Event Store Received, done")
					return
				}
				log.Printf("Receiver A - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		errCh := make(chan error)
		eventsCh, err := receiverB.SubscribeToEventsStore(ctx, channel, "", errCh, kubemq.StartFromFirstEvent())
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
					log.Println("Receiver B - Event Store Received, done")
					return
				}
				log.Printf("Receiver B - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
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
	counter := 0
	for {
		counter++
		result, err := sender.ES().
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetTags(map[string]string{"key1": "value1", "key2": "value2"}).
			SetBody([]byte(fmt.Sprintf("hello kubemq - sending event %d", counter))).
			Send(ctx)
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
