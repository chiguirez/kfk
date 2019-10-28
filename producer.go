package kfk

import (
	"encoding/json"
	"reflect"

	"github.com/Shopify/sarama"
)

// Producer
type KafkaProducer struct {
	producer sarama.SyncProducer
}

// NewKafkaProducer
func NewKafkaProducer(kafkaBrokers []string) (*KafkaProducer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Version = sarama.V1_0_0_0
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaCfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(kafkaBrokers, kafkaCfg)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer}, nil
}

// Send a message to a topic to be scattered using the key
func (p *KafkaProducer) Send(topic string, key string, message interface{}) error {
	bytes, err := p.encode(message)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(bytes),
		Key:     sarama.ByteEncoder([]byte(key)),
		Headers: []sarama.RecordHeader{messageTypeHeader(message)},
	}
	_, _, err = p.producer.SendMessage(producerMessage)

	return err
}

type Marshaler interface {
	MarshalKFK() ([]byte, error)
}

func (p *KafkaProducer) encode(message interface{}) ([]byte, error) {
	marshall, ok := message.(Marshaler)
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
