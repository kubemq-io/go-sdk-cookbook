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
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-delayed-sender"),
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
	channel := "queues.resend"
	resendToChannel := "queues.resend.new-destination"

	sendResult, err := sender.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("queue-message-resend")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	receiver, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-queues-stream-extend-visibility-receiver"),
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

	stream := receiver.NewStreamQueueMessage().SetChannel(channel)
	// get message from the queue
	msg, err := stream.Next(ctx, 5, 10)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	log.Println("resend to new queue")

	err = msg.Resend(resendToChannel)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("done")

	// checking the new channel
	stream = receiver.NewStreamQueueMessage().SetChannel(resendToChannel)
	// get message from the queue
	msg, err = stream.Next(ctx, 5, 10)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	log.Println("resend with new message")
	newMsg := receiver.NewQueueMessage().SetChannel(channel).SetBody([]byte("new message"))
	err = stream.ResendWithNewMessage(newMsg)
	if err != nil {
		log.Fatal(err)
	}
	stream.Close()
	log.Println("checking again the old queue")
	stream = receiver.NewStreamQueueMessage().SetChannel(channel)
	// get message from the queue
	msg, err = stream.Next(ctx, 5, 10)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))

	err = msg.Ack()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("ack and done")
	stream.Close()

}
