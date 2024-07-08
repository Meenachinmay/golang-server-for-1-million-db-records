package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

const (
	numRequests = 95000
	concurrency = 100
	url         = "http://localhost/process"
	payload     = `{"message": "test"}`
)

func main() {
	start := time.Now()

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency) // Limit the number of concurrent goroutines

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release the semaphore slot

			resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(payload)))
			if err != nil {
				fmt.Printf("Request %d failed: %v\n", i, err)
				return
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("Request %d failed to read response: %v\n", i, err)
				return
			}
			fmt.Printf("Request %d: %s\n", i, body)
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	fmt.Printf("Completed %d requests in %v\n", numRequests, duration)
}
