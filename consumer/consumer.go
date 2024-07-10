package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"low-latency-http-server/internal/sqlc"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Consumer struct {
	conn      *amqp.Connection
	store     *sqlc.Queries
	batchSize int
	batch     []string
	mu        sync.Mutex
}

type PostData struct {
	Name string `json:"name"`
}

func NewConsumer(conn *amqp.Connection, store *sqlc.Queries, batchSize int) (*Consumer, error) {
	consumer := &Consumer{conn: conn, store: store, batchSize: batchSize, batch: make([]string, 0, batchSize)}
	if err := consumer.setup(); err != nil {
		return nil, err
	}
	return consumer, nil
}

func (consumer *Consumer) setup() error {
	ch, err := consumer.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	return DeclareDataExchange(ch)
}

func DeclareDataExchange(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		"data_exchange", // name of exchange
		"direct",        // type
		true,
		false,
		false,
		false,
		nil,
	)
}

func DeclarePostDataQueue(ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(
		"post_data", // Name of the queue
		false,       // Durable
		false,       // Delete when unused
		false,       // Exclusive
		false,       // No-wait
		nil,         // Arguments
	)
}

func (consumer *Consumer) ConsumePostData() error {
	ch, err := consumer.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %v", err)
	}
	defer ch.Close()

	queue, err := DeclarePostDataQueue(ch)
	if err != nil {
		return fmt.Errorf("failed to delcare a queue: %v", err)
	}

	if err := ch.QueueBind(queue.Name, "post_data", "data_exchange", false, nil); err != nil {
		return fmt.Errorf("failed to bind a queue: %v", err)
	}

	messages, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to start consuming message from queue: %v", err)
	}

	go func() {
		for d := range messages {
			var payload []string
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				log.Printf("Failed to unmarshal message from queue: %v", err)
				d.Nack(false, false)
				continue
			}

			// if everything is fine, the try to save data
			log.Println("saving data in database...")
			_, err := consumer.store.CreateUser(context.Background(), payload)
			if err != nil {
				log.Printf("Failed to save data in database: %v", err)
				d.Nack(false, true)
				continue
			}
			d.Ack(false)
			log.Printf("successfully processed the batch of size %d\n", len(payload))
		}
	}()

	log.Printf("Waiting for post data [Exchange, Queue] [post_data, %s]\n", queue.Name)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("Received shutdown signal, exiting...\n")

	return nil
}
