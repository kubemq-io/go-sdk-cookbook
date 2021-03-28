package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

// Basic client with client JWT authentication token
// KubeMQ public key verification file is required

const authToken = `eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJDbGllbnRJRCI6ImNsaWVudF9pZCIsIlN0YW5kYXJkQ2xhaW1zIjp7ImV4cCI6MTYxNzQyMDE5NX19.IN2rJjat-L7Ib1zcQPN-hdqmjfGHQ-y559j5L5_Kf8DydQCj0aefWRZWQA8QcE7MRFoPcRL7ToemGHLwCQ7_fyUGQsCp37_yRu-ZZ5fOKByWn4ctfcDipJNUng06Rar6j4IqwWIyNEkn8lXXQeQv1azmeA5l58pPzVagEM7WCcxSNLKYSgdBXLl01-7r2J_V6R_yPqEKkS19Fsa8H1PnJFckflNTX7UrM1mp6-yIT3EaGkk6_jeXAryp1-UlP0NJvpoRvmwZz89lzlQ2wpZ99_T6SEuX9J-8oE5Tb4N87RUAHgm_tsspAEmpkhOWZy5t0z5ye4Y3C8GLqDR_k55CBg`

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

	result, err := client.Ping(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%++v", result)
}
