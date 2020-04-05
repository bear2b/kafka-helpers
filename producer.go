package kafka

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

// Producer represents Kafka's producer
type Producer struct {
	Brokers        string
	saramaProducer sarama.SyncProducer
}

// Init setup initial connection to Kafka
func (producer *Producer) Init() {
	config := sarama.NewConfig()
	// config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	// config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Return.Successes = true // required for SyncProducer or if we need this info for Async
	// config.Producer.Return.Errors = false

	p, err := sarama.NewSyncProducer(strings.Split(producer.Brokers, ","), config)
	if err != nil {
		log.Panicf("Can't connect to any of Kafka brokers %s. Error: %v", producer.Brokers, err)
	}
	producer.saramaProducer = p
}

// SendMessageJSON tries to marshal message to JSON and sends to Kafka
func (producer *Producer) SendMessageJSON(topic string, key string, message interface{}) {
	jsonMessage, err := json.Marshal(message)
	if err != nil {
		log.Fatal(err)
	}
	producer.SendMessage(topic, key, string(jsonMessage))
}

// SendMessage sends a message to Kafka
func (producer *Producer) SendMessage(topic string, key string, message string) {
	if nil == producer.saramaProducer {
		log.Panicf("Producer is not initalized. Please call Producer.Init() first")
	}

	msg := sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(message),
	}

	_, _, err := producer.saramaProducer.SendMessage(&msg)
	if err != nil {
		log.Fatal(err)
	}
}
