package main

import (
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"low-latency-http-server/consumer"
	emitter "low-latency-http-server/emitter"
	"low-latency-http-server/internal/db"
	"low-latency-http-server/internal/sqlc"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	Payload []byte
	Result  chan string
}

type PostData struct {
	Name string `json:"name"`
}

type WorkerPool struct {
	tasks   chan Task
	wg      sync.WaitGroup
	emitter *emitter.Emitter
}

func NewWorkerPool(workerCount int, taskQueueSize int, emitter *emitter.Emitter) *WorkerPool {
	wp := &WorkerPool{
		tasks:   make(chan Task, taskQueueSize),
		emitter: emitter,
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

		err := wp.emitter.Emit(string(task.Payload))
		if err != nil {
			log.Printf("Error marshalling payload: %v", err)
			task.Result <- "failed to process task to queue."
			continue
		}

		// Simulate processing time
		time.Sleep(time.Millisecond * 10)

		response := fmt.Sprintf("Processed by worker %d, to the queue.", id)
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

	// connect to database
	dbConn := db.ConnectToDB()

	// connect to rabbitmq
	rabbitConn, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer rabbitConn.Close()

	emitter, err := emitter.NewEmitter(rabbitConn, "data_exchange", "post_data")
	if err != nil {
		log.Println(errors.New("failed to create emitter: " + err.Error()))
		return
	}

	// starting consumer
	go startConsumer(rabbitConn, sqlc.New(dbConn))

	workerCount := 1000
	taskQueueSize := 100000 // Size of the task queue to handle 100k requests
	wp := NewWorkerPool(workerCount, taskQueueSize, emitter)
	defer wp.Shutdown()

	http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "not a post method", http.StatusMethodNotAllowed)
			return
		}

		var postData PostData
		err := json.NewDecoder(r.Body).Decode(&postData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		payload, err := json.Marshal(postData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

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

// connect to rabbitmq api-gateway
func connect() (*amqp.Connection, error) {
	var counts int64
	var backOffTime = 1 * time.Second

	var connection *amqp.Connection

	for {
		c, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
		if err != nil {
			fmt.Println("RabbitMQ not yet ready...")
			counts++
		} else {
			connection = c
			break
		}

		if counts > 5 {
			fmt.Println(err)
			return nil, err
		}

		backOffTime = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("backing off...")
		time.Sleep(backOffTime)
		continue
	}

	log.Println("Connected to RabbitMQ...")
	return connection, nil
}

func startConsumer(conn *amqp.Connection, store *sqlc.Queries) {
	consumer, err := consumer.NewConsumer(conn, store)
	if err != nil {
		log.Fatalf("Failed to create post data consumer:[startConsumer] %v", err)
	}
	err = consumer.ConsumePostData()
	if err != nil {
		log.Fatalf("Failed to post data:[startConsumer] %v", err)
	}
}

// PurgeQueue removes all messages from the specified queue
func PurgeQueue(conn *amqp.Connection, queueName string) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	// Check the queue to see if there are messages to purge
	queue, err := channel.QueueInspect(queueName)
	if err != nil {
		return err
	}

	if queue.Messages > 0 {
		_, err = channel.QueuePurge(queueName, false)
		if err != nil {
			return err
		}
		log.Printf("Queue %s purged", queueName)
	} else {
		log.Printf("Queue %s is already empty", queueName)
	}

	return nil
}
