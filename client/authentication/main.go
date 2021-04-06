package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

// Basic client with client JWT authentication token
// KubeMQ public key verification file is required
const authToken = `eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjgzNDUzOTQxODV9.l6WdainvCQ8EJ9CFrH7eLBKlZEj69pBpq4T7OXr-NvT_yKWCaq5s3KC1sJrKquyDZvMNKQuGtW3TY8i803kg4V8TsWmmrTAJ5XiTXMg--qMnmmvTu2V1uHd1EaHZXSxHtx58tFtB5v10mRw74qJ18uiROT04YZ0sHKV4ZZG4ZHpvcHrTmZ1mwG-5i2hFol2dR7uad4umkDvFaPlzl4wq-y5-rMBYr8zS-IevWLaL794jxLgjrzV2stQWmbcb5Krrgo0GFS_OjGbt2qjYZ9spPYe6lz6Rktsw9NzbJEYSQnps2Yjemzw-D1o6eY9iPMXnIg3LN4swuxdcXwxz1rRMIg`

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-client-authentication"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC),
		kubemq.WithAuthToken(authToken))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := client.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	channel := "queues.single"

	sendResult, err := client.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("some-simple_queue-queue-message")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	receiveResult, err := client.NewReceiveQueueMessagesRequest().
		SetChannel(channel).
		SetMaxNumberOfMessages(1).
		SetWaitTimeSeconds(1).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Received %d Messages:\n", receiveResult.MessagesReceived)
	for _, msg := range receiveResult.Messages {
		log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	}

}
