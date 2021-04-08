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
	queuesClient, err := kubemq.NewQueuesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-expiration"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channel := "queues.expiration"

	var batch []*kubemq.QueueMessage
	for i := 0; i < 10; i++ {
		batch = append(batch, kubemq.NewQueueMessage().
			SetChannel(channel).SetBody([]byte(fmt.Sprintf("Batch Message %d", i))).
			SetPolicyExpirationSeconds(5).AddTag("message", fmt.Sprintf("%d", i)))
	}

	_, err = queuesClient.Batch(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	result, err := queuesClient.Peek(ctx, &kubemq.ReceiveQueueMessagesRequest{
		ClientID:            "go-sdk-cookbook-queues-expire",
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Peek Received %d Messages\n", result.MessagesReceived)
	time.Sleep(5 * time.Second)
	result, err = queuesClient.Pull(ctx, &kubemq.ReceiveQueueMessagesRequest{
		ClientID:            "go-sdk-cookbook-queues-expire",
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Pull After 5 Seconds Received %d Messages\n", result.MessagesReceived)
}
