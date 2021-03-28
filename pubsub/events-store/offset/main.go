package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-offset-sender"),
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
	randomChannel := uuid.New().String()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		result, err := sender.ES().
			SetChannel(randomChannel).
			SetMetadata("some-metadata").
			SetTags(map[string]string{"key1": "value1", "key2": "value2"}).
			SetBody([]byte(fmt.Sprintf("hello kubemq - sending event store %d", i))).
			Send(ctx)
		if err != nil {
			log.Println(fmt.Sprintf("error sedning event %d, error: %s", i, err))
		}
		log.Printf("Send Message %d ,Result: Id: %s, Sent: %t\n", i, result.Id, result.Sent)
	}
	endTime := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(4)

	// Subscribe to get all messages form start
	go func() {
		defer wg.Done()
		receiver, err := kubemq.NewClient(ctx,
			kubemq.WithAddress("localhost", 50000),
			kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-first"),
			kubemq.WithTransportType(kubemq.TransportTypeGRPC),
			kubemq.WithAutoReconnect(true))

		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			err := receiver.Close()
			if err != nil {
				log.Fatal(err)
			}
		}()
		errCh := make(chan error)
		eventsCh, err := receiver.SubscribeToEventsStore(ctx, randomChannel, "", errCh, kubemq.StartFromFirstEvent())
		if err != nil {
			log.Fatal(err)
			return

		}
		for i := 0; i < 10; i++ {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					log.Println("Receiver start from first - Event Store Received, done")
					return
				}
				log.Printf("Receiver start from first - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	// Subscribe to get all messages from seq 5
	go func() {
		defer wg.Done()
		receiver, err := kubemq.NewClient(ctx,
			kubemq.WithAddress("localhost", 50000),
			kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-seq"),
			kubemq.WithTransportType(kubemq.TransportTypeGRPC),
			kubemq.WithAutoReconnect(true))

		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			err := receiver.Close()
			if err != nil {
				log.Fatal(err)
			}
		}()
		errCh := make(chan error)
		eventsCh, err := receiver.SubscribeToEventsStore(ctx, randomChannel, "", errCh, kubemq.StartFromSequence(5))
		if err != nil {
			log.Fatal(err)
			return

		}
		for i := 0; i < 10; i++ {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					log.Println("Receiver start from sequence - Event Store Received, done")
					return
				}
				log.Printf("Receiver start from sequence - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	// Subscribe to get all messages from 3 seconds ago
	go func() {
		defer wg.Done()
		receiver, err := kubemq.NewClient(ctx,
			kubemq.WithAddress("localhost", 50000),
			kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-time-delta"),
			kubemq.WithTransportType(kubemq.TransportTypeGRPC),
			kubemq.WithAutoReconnect(true))

		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			err := receiver.Close()
			if err != nil {
				log.Fatal(err)
			}
		}()
		errCh := make(chan error)
		eventsCh, err := receiver.SubscribeToEventsStore(ctx, randomChannel, "", errCh, kubemq.StartFromTimeDelta(3*time.Second))
		if err != nil {
			log.Fatal(err)
			return

		}
		for i := 0; i < 10; i++ {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					log.Println("Receiver start from time delta - Event Store Received, done")
					return
				}
				log.Printf("Receiver start from time delta - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	// Subscribe to get all messages from specific time
	go func() {
		defer wg.Done()
		receiver, err := kubemq.NewClient(ctx,
			kubemq.WithAddress("localhost", 50000),
			kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-offset-receiver-start-from-time"),
			kubemq.WithTransportType(kubemq.TransportTypeGRPC),
			kubemq.WithAutoReconnect(true))

		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			err := receiver.Close()
			if err != nil {
				log.Fatal(err)
			}
		}()
		errCh := make(chan error)
		eventsCh, err := receiver.SubscribeToEventsStore(ctx, randomChannel, "", errCh, kubemq.StartFromTime(endTime.Add(-3*time.Second)))
		if err != nil {
			log.Fatal(err)
			return

		}
		for i := 0; i < 10; i++ {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					log.Println("Receiver start from time - Event Store Received, done")
					return
				}
				log.Printf("Receiver start from time - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\nSequence: %d\nTags: %d", event.Id, event.Channel, event.Metadata, event.Body, event.Sequence, len(event.Tags))
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()
}
