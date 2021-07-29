package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()

	Publish("mensagem 222", "teste", producer, nil)
	go DeliveryReport(deliveryChan) //ansync
	producer.Flush(9000)

}

func NewKafkaProducer() *kafka.Producer {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "gokafka_kafka_1:9092",
		"acks": "all",
	})
	if err != nil {
		panic(err)
	}

	return p

}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {

	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, nil)

	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {

	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
			}

		}
	}

}
