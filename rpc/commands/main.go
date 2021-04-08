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
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-rpc-commands-commandsClient"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channel := "commands"

	err = commandsClient.Subscribe(ctx, &kubemq.CommandsSubscription{
		Channel:  channel,
		ClientId: "go-sdk-cookbook-rpc-commands-subscriber",
	}, func(cmd *kubemq.CommandReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Command Received:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", cmd.Id, cmd.Channel, cmd.Metadata, cmd.Body)
			err := commandsClient.Response(ctx, kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now()))
			if err != nil {
				log.Fatal(err)
			}
		}

	})
	if err != nil {
		log.Fatal(err)
	}

	// give some time to connect a receiver
	time.Sleep(time.Second)
	response, err := commandsClient.Send(ctx, kubemq.NewCommand().
		SetId("some-command-id").
		SetChannel(channel).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending a command, please reply")).
		SetTimeout(time.Second))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Response Received:\nCommandID: %s\nExecutedAt:%s\n", response.CommandId, response.ExecutedAt)

}
