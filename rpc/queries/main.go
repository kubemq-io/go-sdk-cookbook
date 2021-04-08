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
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-rpc-queries-queriesClient"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channel := "queries"

	err = queriesClient.Subscribe(ctx, &kubemq.QueriesSubscription{
		Channel:  channel,
		ClientId: "go-sdk-cookbook-rpc-queries-subscriber",
	}, func(cmd *kubemq.QueryReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Query Received:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", cmd.Id, cmd.Channel, cmd.Metadata, cmd.Body)
			err := queriesClient.Response(ctx, kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now()).
				SetMetadata("this is a response").
				SetBody([]byte("got your query, you are good to go")))
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
	response, err := queriesClient.Send(ctx, kubemq.NewQuery().
		SetId("some-query-id").
		SetChannel(channel).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending a query, please reply")).
		SetTimeout(time.Second))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Response Received:\nQueryID: %s\nExecutedAt:%s\n", response.QueryId, response.ExecutedAt)

}
