package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"github.com/nats-io/nuid"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := kubemq.NewQueuesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-single"),
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
	channel := "queues.single"

	done, err := queuesClient.Subscribe(ctx, &kubemq.ReceiveQueueMessagesRequest{
		ClientID:            "go-sdk-cookbook-queues-single",
		Channel:             channel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     2,
	}, func(response *kubemq.ReceiveQueueMessagesResponse, err error) {
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Received %d Messages:\n", response.MessagesReceived)
		for _, msg := range response.Messages {
			log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		messageID := nuid.New().Next()
		sendResult, err := queuesClient.Send(ctx, kubemq.NewQueueMessage().
			SetId(messageID).
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("sending message %d", i))))
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
	}
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	select {

	case <-gracefulShutdown:

	}
	done <- struct{}{}
}
