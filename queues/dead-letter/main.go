package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"github.com/nats-io/nuid"
	"log"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deadLetterChannel := "queues.dead-letter.dead"
	sendCount := 10
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-dead-letter-sender"),
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

	receiver1, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-receiver1"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer func() {
		err := receiver1.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	receiver2, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-receiver2"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer func() {
		err := receiver2.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	for i := 1; i <= sendCount; i++ {
		messageID := nuid.New().Next()
		sendResult, err := sender.NewQueueMessage().
			SetId(messageID).
			SetChannel("queues.a").
			SetBody([]byte(fmt.Sprintf("sending message %d", i))).
			SetPolicyMaxReceiveCount(1).
			SetPolicyMaxReceiveQueue(deadLetterChannel).
			Send(ctx)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			stream := receiver1.NewStreamQueueMessage().SetChannel("queues.a")
			// get message from the queue
			msg, err := stream.Next(ctx, 10000, 5)
			if err != nil {
				log.Println("No new messages for ReceiverA")
				return
			}
			if msg == nil {
				return
			}
			log.Printf("Queue: ReceiverA,MessageID: %s, Body: %s,Seq: %d - send to queue receiver b", msg.MessageID, string(msg.Body), msg.Attributes.Sequence)
			// do some work and resend to the next queue
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel("queues.b")
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Fatal(err)
			}
			stream.Close()

		}

	}()

	go func() {
		defer wg.Done()
		for {
			stream := receiver2.NewStreamQueueMessage().SetChannel("queues.b")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverB")
				return
			}
			if msg == nil {
				return
			}
			log.Printf("Queue: ReceiverB,MessageID: %s, Body: %s - rejecting message", msg.MessageID, string(msg.Body))
			time.Sleep(10 * time.Millisecond)
			// do some work that fails
			err = msg.Reject()
			if err != nil {
				log.Fatal(err)
			}
			stream.Close()
		}

	}()

	wg.Wait()

	res, err := sender.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		RequestID:           "some_request",
		ClientID:            "sender-client_id",
		Channel:             deadLetterChannel,
		MaxNumberOfMessages: int32(sendCount),
		WaitTimeSeconds:     2,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.IsError {
		log.Fatal(res.Error)
	}
	log.Printf("Dead Letter Messages - %d: Expried - %d", res.MessagesReceived, res.MessagesExpired)
	for i := 0; i < len(res.Messages); i++ {
		log.Printf("MessageID: %s, Body: %s", res.Messages[i].MessageID, res.Messages[i].Body)
	}
}
