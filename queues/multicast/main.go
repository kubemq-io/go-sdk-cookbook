package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-multicast-client"),
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
	// sending to 3 queues in one request
	multicastChannel := "queue.a;queue.b;queue.c"
	sendResult, err := client.NewQueueMessage().
		SetChannel(multicastChannel).
		SetBody([]byte("some-simple_queue-queue-message")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	receiveResult, err := client.NewReceiveQueueMessagesRequest().
		SetChannel("queue.a").
		SetMaxNumberOfMessages(1).
		SetWaitTimeSeconds(1).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Queue A Received %d Messages:\n", receiveResult.MessagesReceived)
	for _, msg := range receiveResult.Messages {
		log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	}

	receiveResult, err = client.NewReceiveQueueMessagesRequest().
		SetChannel("queue.b").
		SetMaxNumberOfMessages(1).
		SetWaitTimeSeconds(1).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Queue B Received %d Messages:\n", receiveResult.MessagesReceived)
	for _, msg := range receiveResult.Messages {
		log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	}
	receiveResult, err = client.NewReceiveQueueMessagesRequest().
		SetChannel("queue.c").
		SetMaxNumberOfMessages(1).
		SetWaitTimeSeconds(1).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Queue C Received %d Messages:\n", receiveResult.MessagesReceived)
	for _, msg := range receiveResult.Messages {
		log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	}
}
