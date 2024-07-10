package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
	"log"
	"sync"
	"time"

	users "low-latency-http-server/grpc-user"
)

func main() {
	// Load gRPC configuration
	configData, err := ioutil.ReadFile("grpc_config.json")
	if err != nil {
		log.Fatalf("Failed to read gRPC config file: %v", err)
	}

	// Dial the gRPC server with the service config
	conn, err := grpc.Dial(
		"localhost:80",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(string(configData)),
	)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := users.NewPostDataServiceClient(conn)

	// Perform gRPC requests
	performRequests(client, 1000000)
}

func performRequests(client users.PostDataServiceClient, requestCount int) {
	var wg sync.WaitGroup
	var successCount int64
	var failCount int64
	var mu sync.Mutex
	concurrency := 200 // Number of concurrent requests
	batchSize := requestCount / concurrency

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < batchSize; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				req := &users.PostDataRequest{Name: fmt.Sprintf("Chinmay anand %d", i*batchSize+j)}
				_, err := client.Process(ctx, req)
				mu.Lock()
				if err != nil {
					failCount++
				} else {
					successCount++
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	log.Printf("Total requests: %d, Successful: %d, Failed: %d", requestCount, successCount, failCount)
}
