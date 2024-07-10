package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"log"
	"low-latency-http-server/consumer"
	emitter "low-latency-http-server/emitter"
	users "low-latency-http-server/grpc-user"
	"low-latency-http-server/internal/db"
	"low-latency-http-server/internal/sqlc"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
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
	tasks     chan Task
	wg        sync.WaitGroup
	emitter   *emitter.Emitter
	batchSize int
	dbConn    *sqlc.Queries
	mu        sync.Mutex
	batch     []string
}

func NewWorkerPool(workerCount int, taskQueueSize int, batchSize int, emitter *emitter.Emitter, dbConn *sqlc.Queries) *WorkerPool {
	wp := &WorkerPool{
		tasks:     make(chan Task, taskQueueSize),
		emitter:   emitter,
		batchSize: batchSize,
		dbConn:    dbConn,
		batch:     make([]string, 0, batchSize),
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
		var postData PostData
		err := json.Unmarshal(task.Payload, &postData)
		if err != nil {
			log.Printf("Error unmarshalling payload: %v", err)
			task.Result <- "failed to unmarshal payload."
			close(task.Result)
			continue
		}

		wp.mu.Lock()
		wp.batch = append(wp.batch, postData.Name)
		if len(wp.batch) >= wp.batchSize {
			batch := wp.batch
			wp.batch = make([]string, 0, wp.batchSize)
			wp.mu.Unlock()
			wp.processBatch(batch)
		} else {
			wp.mu.Unlock()
		}

		response := fmt.Sprintf("Processed by worker %d, to the queue.", id)
		task.Result <- response
		close(task.Result)
	}

	// Process any remaining tasks in the batch
	wp.mu.Lock()
	if len(wp.batch) > 0 {
		wp.processBatch(wp.batch)
		wp.batch = nil
	}
	wp.mu.Unlock()
}

func (wp *WorkerPool) processBatch(batch []string) {
	batchPayload, err := json.Marshal(batch)
	if err != nil {
		log.Printf("Error marshalling batch: %v", err)
		return
	}

	err = wp.emitter.Emit(string(batchPayload))
	if err != nil {
		log.Printf("Error emitting batch to RabbitMQ: %v", err)
		return
	}

	log.Printf("Successfully emitted batch of size %d", len(batch))
}

func (wp *WorkerPool) SubmitTask(task Task) {
	wp.tasks <- task
}

func (wp *WorkerPool) Shutdown() {
	close(wp.tasks)
	wp.wg.Wait()
}

type server struct {
	users.UnimplementedPostDataServiceServer
	workerPool     *WorkerPool
	circuitBreaker *gobreaker.CircuitBreaker
}

func (s *server) Process(ctx context.Context, req *users.PostDataRequest) (*users.PostDataResponse, error) {

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resultChan := make(chan string)
	task := Task{
		Payload: payload,
		Result:  resultChan,
	}

	atomic.AddInt64(&requestCount, 1)

	s.workerPool.SubmitTask(task)

	select {
	case response := <-resultChan:
		log.Printf("processed payload: %s\n", response)
		return &users.PostDataResponse{Response: response}, nil
	case <-time.After(time.Second * 5): // Timeout for processing
		return nil, fmt.Errorf("processing timeout")
	}

}

var requestCount int64

func main() {
	// Set GOMAXPROCS to the number of available CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Create a circuit breaker
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "gRPC Circuit Breaker",
		MaxRequests: 1000,             // Allow 1000 requests during the half-open state
		Interval:    30 * time.Second, // Reset the failure count after this interval
		Timeout:     10 * time.Second, // Timeout duration for the open state
	})

	// connect to database
	dbConn := db.ConnectToDB()

	// connect to rabbitmq
	rabbitConn, err := connect()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	defer rabbitConn.Close()

	eventEmitter, err := emitter.NewEmitter(rabbitConn, "data_exchange", "post_data")
	if err != nil {
		log.Println(errors.New("failed to create emitter: " + err.Error()))
		return
	}

	// starting consumer
	go startConsumer(rabbitConn, sqlc.New(dbConn))

	workerCount := runtime.NumCPU()
	taskQueueSize := 10000 // Size of the task queue to handle 100k requests
	batchSize := 100
	wp := NewWorkerPool(workerCount, taskQueueSize, batchSize, eventEmitter, sqlc.New(dbConn))
	defer wp.Shutdown()

	grpcServerOptions := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(1000),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Hour,
			Timeout:           20 * time.Second,
		}),
	}

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer(grpcServerOptions...)
	users.RegisterPostDataServiceServer(grpcServer, &server{workerPool: wp, circuitBreaker: cb})
	reflection.Register(grpcServer)

	log.Println("gRPC server listening on port 50051")
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	grpcServer.GracefulStop()
	log.Println("Server gracefully stopped")
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
	cc, err := consumer.NewConsumer(conn, store)
	if err != nil {
		log.Fatalf("Failed to create post data consumer:[startConsumer] %v", err)
	} else {
		log.Println("Consumer created...")
	}
	err = cc.ConsumePostData()
	if err != nil {
		log.Fatalf("Failed to post data:[startConsumer] %v", err)
	} else {
		log.Println("Post data consumed...")
	}
}
