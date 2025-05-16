package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "group1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("Error creating consumer:", err)
	}
	defer c.Close()

	err = c.Subscribe("user-events", nil)
	if err != nil {
		log.Fatal("Subscription error:", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Consumer has started. Looking for mssgs")

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("terminating", sig)
			run = false
		default:
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message received from Kakfa: Topic: %s Value: %s\n", *msg.TopicPartition.Topic, string(msg.Value))
			} else {
				fmt.Printf("Consumer error: %v (%v)", err, msg)
			}
		}
	}

}
