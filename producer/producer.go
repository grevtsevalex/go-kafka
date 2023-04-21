package main

import (
	"context"
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

func writeToKafka(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("could not read body: %s\n", err)
	}
	wr:= kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic: topic,
	})
	wrErr := wr.WriteMessages(ctx, kafka.Message{
		Value: []byte("this is message" + string(body)),
	})
	if wrErr != nil {
		panic("could not write message " + wrErr.Error())
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
