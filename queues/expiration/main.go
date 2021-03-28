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
		kubemq.WithClientId("go-sdk-cookbook-queues-expiration-sender"),
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
	channel := "queues.expiration"

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

	batch := sender.NewQueueMessages()
	for i := 0; i < 10; i++ {
		batch.Add(sender.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("Batch Message %d", i))).
			SetPolicyExpirationSeconds(2))

	}
	batchResult, err := batch.Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, sendResult := range batchResult {
		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
	}
	// waiting for 5 seconds
	time.Sleep(5 * time.Second)

	go func() {
		for {
			receiveResult, err := workerA.NewReceiveQueueMessagesRequest().
				SetChannel(channel).
				SetMaxNumberOfMessages(10).
				SetWaitTimeSeconds(2).
				Send(ctx)
			if err != nil {
				return
			}
			log.Printf("Worker A Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
			log.Printf("Worker A notified of %d Expired Messages:\n", receiveResult.MessagesExpired)

		}

	}()

	go func() {
		for {
			receiveResult, err := workerB.NewReceiveQueueMessagesRequest().
				SetChannel(channel).
				SetMaxNumberOfMessages(10).
				SetWaitTimeSeconds(2).
				Send(ctx)
			if err != nil {
				return
			}
			log.Printf("Worker B Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
			log.Printf("Worker B notified of %d Expired Messages:\n", receiveResult.MessagesExpired)
		}

	}()
	time.Sleep(3 * time.Second)
}
