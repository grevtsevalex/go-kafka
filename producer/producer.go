package main

import (
	"context"
	"time"
	"github.com/segmentio/kafka-go"
	"net/http"
	"errors"
	"fmt"
	"os"
)

func writeToKafka(w http.ResponseWriter, r *http.Request) {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "my-topic", 0)
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	conn.WriteMessages(kafka.Message{Value: []byte("message write")})
}

func main() {

	http.HandleFunc("/write", writeToKafka)

	err := http.ListenAndServe(":3333", nil)
  if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)}
}