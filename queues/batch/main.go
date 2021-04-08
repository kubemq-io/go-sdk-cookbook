package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := kubemq.NewQueuesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-batch"),
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
	channel := "queues.batch"
	var batch []*kubemq.QueueMessage
	for i := 0; i < 10; i++ {
		batch = append(batch, kubemq.NewQueueMessage().
			SetChannel(channel).SetBody([]byte(fmt.Sprintf("Batch Message %d", i))))
	}

	_, err = queuesClient.Batch(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	result, err := queuesClient.Pull(ctx, &kubemq.ReceiveQueueMessagesRequest{
		ClientID:            "go-sdk-cookbook-queues-batch",
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     2,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Received %d Messages:\n", result.MessagesReceived)
	for _, msg := range result.Messages {
		log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	}

}
