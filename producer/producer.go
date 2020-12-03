package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/Shopify/sarama"
)

// Producer.
type Producer struct {
	producer sarama.SyncProducer
	client   sarama.Client
}

var errCreateProducer = fmt.Errorf("error during producer creation")

// New.
func New(kafkaBrokers []string) (*Producer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Version = sarama.V1_0_0_0
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaCfg.Producer.Return.Successes = true

	kafkaClient, err := sarama.NewClient(kafkaBrokers, kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCreateProducer, err)
	}

	producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCreateProducer, err)
	}

	return &Producer{producer: producer, client: kafkaClient}, nil
}

var errSendProducer = fmt.Errorf("error sending message")

// Send a message to a topic to be scattered using the key.
func (p *Producer) Send(topic, key string, message interface{}) error {
	bytes, err := p.encode(message)
	if err != nil {
		return fmt.Errorf("%w:%v", errSendProducer, err)
	}

	producerMessage := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(bytes),
		Key:       sarama.ByteEncoder(key),
		Headers:   []sarama.RecordHeader{messageTypeHeader(message)},
		Timestamp: time.Now(),
	}

	_, _, err = p.producer.SendMessage(producerMessage)
	if err != nil {
		return fmt.Errorf("%w:%v", errSendProducer, err)
	}

	return nil
}

func (p Producer) HealthCheck(_ context.Context) bool {
	controller, err := p.client.Controller()
	if err != nil {
		return false
	}

	connected, err := controller.Connected()
	if err != nil {
		return false
	}

	return connected
}

// Marshaller is an interface to serialize messages to kfkTopics.
type Marshaller interface {
	MarshalKFK() ([]byte, error)
}

func (p *Producer) encode(message interface{}) ([]byte, error) {
	marshall, ok := message.(Marshaller)
	if ok {
		return marshall.MarshalKFK()
	}

	return json.Marshal(message)
}

func messageTypeHeader(message interface{}) sarama.RecordHeader {
	return sarama.RecordHeader{
		Key:   []byte("@type"),
		Value: []byte(messageType(message)),
	}
}

func messageType(m interface{}) string {
	rType := reflect.TypeOf(m)
	if rType.Kind() == reflect.Ptr {
		return rType.Elem().Name()
	}

	return rType.Name()
}
