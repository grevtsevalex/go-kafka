package main

import (
	"context"
	"time"
	"github.com/segmentio/kafka-go"
	"fmt"
)


func main() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "my-topic", 0)
	conn.SetReadDeadline(time.Now().Add(time.Second * 8))

	message, _ := conn.ReadMessage(1e3)
	fmt.Println(string(message.Value))
}
