package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Task struct {
	Payload []byte
	Result  chan<- string
}

type WorkerPool struct {
	tasks chan Task
	wg    sync.WaitGroup
}

func NewWorkerPool(workerCount int) *WorkerPool {
	wp := &WorkerPool{
		tasks: make(chan Task, workerCount),
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
		time.Sleep(time.Millisecond * 10)

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
	workerCount := runtime.NumCPU() * 4
	wp := NewWorkerPool(workerCount)
	defer wp.Shutdown()

	http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "not a post method", http.StatusMethodNotAllowed)
			return
		}

		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to load payload", http.StatusInternalServerError)
			return
		}

		resultChan := make(chan string)
		task := Task{
			Payload: payload,
			Result:  resultChan,
		}

		atomic.AddInt64(&requestCount, 1)

		wp.SubmitTask(task)

		response := <-resultChan
		log.Printf("processed payload: %s\n", response)
	})

	log.Printf("listening on port 8080\n")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
