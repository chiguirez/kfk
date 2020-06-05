package kfk

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/Shopify/sarama"
)

// Producer.
type KafkaProducer struct {
	producer sarama.SyncProducer
	client   sarama.Client
}

// NewKafkaProducer.
func NewKafkaProducer(kafkaBrokers []string) (*KafkaProducer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Version = sarama.V1_0_0_0
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaCfg.Producer.Return.Successes = true

	kafkaClient, err := sarama.NewClient(kafkaBrokers, kafkaCfg)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer: producer, client: kafkaClient}, nil
}

// Send a message to a topic to be scattered using the key.
func (p *KafkaProducer) Send(topic string, key string, message interface{}) error {
	bytes, err := p.encode(message)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(bytes),
		Key:     sarama.ByteEncoder(key),
		Headers: []sarama.RecordHeader{messageTypeHeader(message)},
	}
	_, _, err = p.producer.SendMessage(producerMessage)

	return err
}

func (p KafkaProducer) Check(_ context.Context) bool {
	return !p.client.Closed()
}

// Marshaller is an interface to serialize messages to kfkTopics.
type Marshaller interface {
	MarshalKFK() ([]byte, error)
}

func (p *KafkaProducer) encode(message interface{}) ([]byte, error) {
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
