package kafka

import (
	"context"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

// IncomingMessage represent a message
type IncomingMessage struct {
	Topic string
	Value string
}

// ConsumerHandlerFunc will be called to process a message
type ConsumerHandlerFunc func(context.Context, IncomingMessage) bool

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	Brokers  string
	ClientID string
	Group    string
	// ready    chan bool
	Ctx      context.Context
	client   sarama.ConsumerGroup
	handlers map[string]ConsumerHandlerFunc
}

// Run do message consuming and call message handler function
func (consumer *Consumer) Run() {
	if len(consumer.handlers) == 0 {
		log.Fatalln("Error: No registered handlers. Please call Consumer.AddHandler(..) first")
	}

	// get all topics from handlers
	topics := make([]string, len(consumer.handlers))
	i := 0
	for key := range consumer.handlers {
		topics[i] = key
		i++
	}

	config := sarama.NewConfig()
	config.Version, _ = sarama.ParseKafkaVersion("2.1.1")
	config.ClientID = consumer.ClientID
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup(strings.Split(consumer.Brokers, ","), consumer.Group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	consumer.client = client
	// consumer.ready = make(chan bool)

	for {
		if err := client.Consume(consumer.Ctx, topics, consumer); err != nil {
			log.Panicf("Error from consumer: %v", err)
		}
		// check if context was cancelled, signaling that the consumer should stop
		if consumer.Ctx.Err() != nil {
			return
		}
		log.Println("in the main loop...")
		// consumer.ready = make(chan bool)
	}
}

// AddHandler registers new topic handler
func (consumer *Consumer) AddHandler(topic string, handler ConsumerHandlerFunc) {
	if nil == consumer.handlers {
		consumer.handlers = make(map[string]ConsumerHandlerFunc)
	}
	consumer.handlers[topic] = handler
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	// close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// This method called in dedicated goroutine
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		session.MarkMessage(message, "")
		consumer.handlers[claim.Topic()](consumer.Ctx, IncomingMessage{Topic: message.Topic, Value: string(message.Value)})
	}
	return nil
}
