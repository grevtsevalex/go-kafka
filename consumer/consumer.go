package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"fmt"
)
const (
	topic = "my-topic"
	broker = "localhost:9092"
)

func main() {
	ctx := context.Background()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic: topic,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
	fmt.Println("received: ", string(msg.Value))		
	}
}
