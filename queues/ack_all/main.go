package main

import (
	"context"
	"fmt"
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
		kubemq.WithClientId("go-sdk-cookbook-queues-ack-all-sender"),
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

	channel := "queues.ack-all"

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

	ackClient, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-ack-client"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := ackClient.Close()
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
	receiveResult, err := ackClient.NewAckAllQueueMessagesRequest().
		SetChannel(channel).
		SetWaitTimeSeconds(2).
		Send(ctx)
	if err != nil {
		return
	}
	log.Printf("ack Messages: %d completed\n", receiveResult.AffectedMessages)

	// try Consuming the messages
	time.Sleep(time.Second)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		receiveResult, err := workerA.NewReceiveQueueMessagesRequest().
			SetChannel(channel).
			SetMaxNumberOfMessages(1).
			SetWaitTimeSeconds(1).
			Send(ctx)
		if err != nil {
			log.Printf("WorkerA Error: %s\n", err.Error())
			return
		}
		log.Printf("Worker A Received %d Messages:\n", receiveResult.MessagesReceived)
	}()

	go func() {
		defer wg.Done()
		receiveResult, err := workerB.NewReceiveQueueMessagesRequest().
			SetChannel(channel).
			SetMaxNumberOfMessages(1).
			SetWaitTimeSeconds(1).
			Send(ctx)
		if err != nil {
			log.Printf("WorkerB Error: %s\n", err.Error())
			return
		}
		log.Printf("Worker B Received %d Messages:\n", receiveResult.MessagesReceived)

	}()
	wg.Wait()
}
