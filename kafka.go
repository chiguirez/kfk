package kfk

import (
	"encoding/json"
	"reflect"

	"github.com/Shopify/sarama"
)

type KafkaProducer struct {
	// TODO add async producer
	producer sarama.SyncProducer
}

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

type Message interface {
	ID() string
}

type typedMessage struct {
	Message     `json:"message_body"`
	MessageType string `json:"message_type"`
}

func (tm typedMessage) Raw() []byte {
	raw, _ := json.Marshal(tm)

	return raw
}

func (tm typedMessage) ID() string {
	return tm.Message.ID()
}

func (p *KafkaProducer) Send(topic string, message Message) error {
	typedMessage := typedMessage{message, messageType(message)}

	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(typedMessage.Raw()),
		Key:   sarama.ByteEncoder([]byte(typedMessage.ID())),
	}
	_, _, err := p.producer.SendMessage(producerMessage)

	return err
}

// TODO this responsibility should be in a different service
func messageType(m Message) string {
	rType := reflect.TypeOf(m)
	if rType.Kind() == reflect.Ptr {
		return rType.Elem().Name()
	}

	return rType.Name()
}
