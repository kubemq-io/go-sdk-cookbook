package main

import (
	"context"
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
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-multicast-mix-client"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	eventsReceiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-multicast-mix-events-receiver"),
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
		kubemq.WithClientId("go-sdk-cookbook-queues-multicast-mix-events-store-receiver"),
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
		kubemq.WithClientId("go-sdk-cookbook-queues-multicast-mix-queue-receiver"),
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
					log.Println("Events Receiver - Event Received, done")
					return
				}
				log.Printf("Events Receiver - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
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
					log.Println("Events Store Receiver - Event Received, done")
					return
				}
				log.Printf("Events Store Receiver - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
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
	time.Sleep(time.Second)
	multicastChannel := "q1;events:e1;events_store:es1"
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	for {

		sendResult, err := client.NewQueueMessage().
			SetChannel(multicastChannel).
			SetBody([]byte("some-simple_queue-queue-message")).
			Send(ctx)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
		select {
		case <-gracefulShutdown:
			break
		default:
			time.Sleep(time.Second)
		}
	}

}
