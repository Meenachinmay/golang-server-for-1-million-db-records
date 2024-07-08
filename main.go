package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	Payload []byte
	Result  chan string
}

type WorkerPool struct {
	tasks chan Task
	wg    sync.WaitGroup
}

func NewWorkerPool(workerCount int, taskQueueSize int) *WorkerPool {
	wp := &WorkerPool{
		tasks: make(chan Task, taskQueueSize),
	}

	for i := 0; i < workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return wp
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	for task := range wp.tasks {
		// Simulate processing time
		time.Sleep(time.Millisecond * 10)

		// Process the payload and send the response
		response := fmt.Sprintf("Processed by worker %d", id)
		task.Result <- response
	}

}

func (wp *WorkerPool) SubmitTask(task Task) {
	wp.tasks <- task
}

func (wp *WorkerPool) Shutdown() {
	close(wp.tasks)
	wp.wg.Wait()
}

var requestCount int64

func main() {
	workerCount := 1000
	taskQueueSize := 100000 // Size of the task queue to handle 100k requests
	wp := NewWorkerPool(workerCount, taskQueueSize)
	defer wp.Shutdown()

	http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "not a post method", http.StatusMethodNotAllowed)
			return
		}

		payload, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to load payload", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close() // Ensure the request body is closed

		resultChan := make(chan string)
		task := Task{
			Payload: payload,
			Result:  resultChan,
		}

		atomic.AddInt64(&requestCount, 1)

		wp.SubmitTask(task)

		select {
		case response := <-resultChan:
			log.Printf("processed payload: %s\n", response)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"response": response})
		case <-time.After(time.Second * 5): // Timeout for processing
			http.Error(w, "processing timeout", http.StatusGatewayTimeout)
		}
	})

	log.Printf("listening on port 8080\n")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
