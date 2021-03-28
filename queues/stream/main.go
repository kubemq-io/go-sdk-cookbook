package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"sync"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-stream-sender"),
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

	receiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-delayed-sender"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer func() {
		err := receiver.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channel := "queues.stream"
	batch := sender.NewQueueMessages()
	for i := 0; i < 1000; i++ {
		batch.Add(kubemq.NewQueueMessage().SetChannel(channel).SetBody([]byte("some-stream_simple-queue-message")))
	}
	_, err = batch.Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("1000 messages sent")

	wg := sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				stream := receiver.NewStreamQueueMessage().SetChannel(channel)

				// get message from the queue
				msg, err := stream.Next(ctx, 15, 10)
				if err != nil {
					log.Fatal(err)
				}

				err = msg.Ack()
				if err != nil {
					log.Fatal(err)
				}
				stream.Close()
			}
		}()
	}
	wg.Wait()
	log.Printf("1000 messages received")
}
