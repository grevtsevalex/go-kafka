package main

import (
	"context"
	"time"
	"github.com/segmentio/kafka-go"
	"net/http"
	"errors"
	"fmt"
	"os"
	"io/ioutil"
)

const (
	topic = "my-topic"
	broker = "localhost:9092"
)

func writeToKafka(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("could not read body: %s\n", err)
	}
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic: topic,
	})
	err := w.WriteMessages(ctx, kafka.Message{
		Key: []byte(strconv.Itoa(i)),
		Value: []byte("this is message" + body),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}
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
