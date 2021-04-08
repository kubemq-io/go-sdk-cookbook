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
		kubemq.WithClientId("go-sdk-cookbook-queues-stream-extend"),
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

	channel := "queues.extend"

	doneA, err := queuesClient.TransactionStream(ctx, &kubemq.QueueTransactionMessageRequest{
		ClientID:          "go-sdk-cookbook-queues-stream-visibility",
		Channel:           channel,
		VisibilitySeconds: 1,
		WaitTimeSeconds:   10,
	}, func(response *kubemq.QueueTransactionMessageResponse, err error) {
		if err != nil {

		} else {
			log.Printf("Queue: Receiver MessageID: %s, Body: %s - extend message", response.Message.MessageID, string(response.Message.Body))
			err = response.ExtendVisibilitySeconds(5)
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(1 * time.Second)
			log.Printf("Queue: Receiver MessageID: %s, Body: %s - ack message", response.Message.MessageID, string(response.Message.Body))
			err = response.Ack()
			if err != nil {
				log.Fatal(err)
			}
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
	doneA <- struct{}{}

}
