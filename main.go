package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type Env struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
	rdb      *redis.Client
}

func (e *Env) setKeyInStore(c *gin.Context) {
	store := c.Param("store")
	key := c.Param("key")
	body, err := io.ReadAll(c.Request.Body)

	if err != nil {
		panic("failed to read body")
	}

	fmt.Fprintf(os.Stdout, "Got body: %v\n", body)

	completionChan := make(chan kafka.Event)

	topic := "set-key-in-store"
	e.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0},
		Value:          body,
		Key:            []byte(store + ":" + key),
		Timestamp:      time.Time{},
		TimestampType:  0,
		Opaque:         nil,
		Headers:        []kafka.Header{},
	}, completionChan)

	completed := <-completionChan

	fmt.Fprintf(os.Stdout, "Delivered: %v\n", completed)
}

func writeStrings(w io.Writer, strings []string) {
	for _, s := range strings {
		fmt.Fprintf(w, "%v\n", s)
	}
}

func (e *Env) getKeysInStore(c *gin.Context) {
	store := c.Param("store:")
	keys, cursor, err := e.rdb.Scan(ctx, 0, store, 10).Result()
	if err != nil {
		c.AbortWithError(500, err)
	}
	header := c.Writer.Header()
	header.Set("Transfer-Encoding", "chunked")
	writeStrings(c.Writer, keys)
	var next = cursor
	for next != 0 {
		keys, cursor, err := e.rdb.Scan(ctx, next, store, 10).Result()
		if err != nil {
			c.AbortWithError(500, err)
		}
		writeStrings(c.Writer, keys)
		next = cursor
	}
	c.Writer.(http.Flusher).Flush()
}

func (e *Env) subscribe() {
	err := e.consumer.SubscribeTopics([]string{"set-key-in-store"}, nil)

	if err != nil {
		panic("failed to subscribe")
	}
	run := true
	for run == true {
		ev := e.consumer.Poll(100)
		switch event := ev.(type) {
		case *kafka.Message:
			val, err := e.rdb.Set(ctx, string(event.Key[:]), event.Value, 0).Result()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Redis Set error: %v\n", err)
			}
			fmt.Fprintf(os.Stdout, "Redis set result: %v\n", val)
			// application-specific processing
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", event)
			run = false
		}
	}

	e.consumer.Close()
}

func (e *Env) getKeyInStore(c *gin.Context) {
	store := c.Param("store")
	key := c.Param("key")

	val, err := e.rdb.Get(ctx, store+":"+key).Result()

	if err != nil {
		c.AbortWithError(500, err)
	}

	fmt.Fprint(c.Writer, val)
}

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "producer",
		"acks":              "all"})

	if err != nil {
		panic("couldn't init producer")
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest"})

	if err != nil {
		panic("couldn't init consumer")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	env := Env{producer: producer, consumer: consumer, rdb: rdb}

	go env.subscribe()

	router := gin.Default()
	router.GET("/store/:store/key/:key", env.getKeyInStore)
	router.POST("/store/:store/key/:key", env.setKeyInStore)
	router.GET("/store/:store", env.getKeysInStore)
	router.Run("localhost:8000")
}
