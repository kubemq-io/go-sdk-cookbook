package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-peek-sender"),
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
	channel := "queues.peek"

	workerA, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-worker-a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := workerA.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	workerB, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-worker-b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := workerB.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	peekClient, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-peek-client"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := peekClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	batch := sender.NewQueueMessages()
	for i := 0; i < 10; i++ {
		batch.Add(sender.NewQueueMessage().
			SetChannel(channel).SetBody([]byte(fmt.Sprintf("Batch Message %d", i))))
	}
	batchResult, err := batch.Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, sendResult := range batchResult {
		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
	}

	// Peeking the messaging
	time.Sleep(time.Second)
	receiveResult, err := peekClient.NewReceiveQueueMessagesRequest().
		SetChannel(channel).
		SetMaxNumberOfMessages(10).
		SetWaitTimeSeconds(2).
		SetIsPeak(true).
		Send(ctx)
	if err != nil {
		return
	}
	log.Printf("Peak Client Received %d Messages:\n", receiveResult.MessagesReceived)
	for _, msg := range receiveResult.Messages {
		log.Printf("Peaking MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	}

	// Consuming the messages
	time.Sleep(time.Second)

	go func() {
		for {
			receiveResult, err := workerA.NewReceiveQueueMessagesRequest().
				SetChannel(channel).
				SetMaxNumberOfMessages(1).
				SetWaitTimeSeconds(2).
				Send(ctx)
			if err != nil {
				return
			}
			log.Printf("Worker A Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
		}

	}()

	go func() {
		for {
			receiveResult, err := workerB.NewReceiveQueueMessagesRequest().
				SetChannel(channel).
				SetMaxNumberOfMessages(1).
				SetWaitTimeSeconds(2).
				Send(ctx)
			if err != nil {
				return
			}
			log.Printf("Worker B Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
		}

	}()
	time.Sleep(time.Second)
}
