package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-rpc-commands-client"),
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
	channel := "queries"
	go func() {
		errCh := make(chan error)
		queriesCh, err := client.SubscribeToQueries(ctx, channel, "", errCh)
		if err != nil {

			log.Fatal(err)
		}
		for {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case query, more := <-queriesCh:
				if !more {
					log.Println("Query Received, done")
					return
				}
				log.Printf("Query Received:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", query.Id, query.Channel, query.Metadata, query.Body)
				err := client.NewResponse().
					SetRequestId(query.Id).
					SetResponseTo(query.ResponseTo).
					SetExecutedAt(time.Now()).
					SetMetadata("this is a response").
					SetBody([]byte("got your query, you are good to go")).
					Send(ctx)
				if err != nil {
					log.Fatal(err)
				}
			case <-ctx.Done():
				return
			}
		}

	}()
	// give some time to connect a receiver
	time.Sleep(1 * time.Second)
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	counter := 0
	for {
		counter++
		response, err := client.NewQuery().
			SetId("some-query-id").
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending a query, please reply")).
			SetTimeout(1 * time.Second).
			Send(ctx)
		if err != nil {
			log.Println(fmt.Sprintf("error sending query %d, error: %s", counter, err))
		} else {
			log.Printf("Response Received:\nQueryID: %s\nExecutedAt:%s\nMetadata: %s\nBody: %s\n", response.QueryId, response.ExecutedAt, response.Metadata, response.Body)
		}

		select {
		case <-gracefulShutdown:
			break
		default:
			time.Sleep(time.Second)
		}
	}

}
