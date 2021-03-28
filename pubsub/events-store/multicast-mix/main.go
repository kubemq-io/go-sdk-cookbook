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
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-multicast-sender"),
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

	eventsReceiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-multicast-mix-events-receiver"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsReceiver.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	eventsStoreReceiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-multicast-mix-events-store-receiver"),
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
	queueReceiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-multicast-mix-queue-receiver"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAutoReconnect(true))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queueReceiver.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		errCh := make(chan error)
		eventsCh, err := eventsReceiver.SubscribeToEvents(ctx, "e1", "", errCh)
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
					log.Println("Events Receiver - Event Store Received, done")
					return
				}
				log.Printf("Events Receiver - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		errCh := make(chan error)
		eventsCh, err := eventsStoreReceiver.SubscribeToEventsStore(ctx, "es1", "", errCh, kubemq.StartFromFirstEvent())
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
					log.Println("Events Store Receiver - Event Store Received, done")
					return
				}
				log.Printf("Events Store Receiver - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {

		for {
			receiveResult, err := queueReceiver.NewReceiveQueueMessagesRequest().
				SetChannel("q1").
				SetMaxNumberOfMessages(1).
				SetWaitTimeSeconds(5).
				Send(ctx)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Queue Receiever Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	multicastChannel := "es1;events:e1;queues:q1"
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	counter := 0
	for {
		counter++
		result, err := sender.ES().
			SetChannel(multicastChannel).
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
