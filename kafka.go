package kfk

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/Shopify/sarama"
)

// Consumer

type KafkaDecodedMessage struct {
	Body        interface{} `json:"message_body"`
	MessageType string      `json:"message_type"`
}

//go:generate moq -out message_handler_mock.go . messageHandler
type messageHandler interface {
	Handle(KafkaDecodedMessage) error
}

type MessageHandlerList map[string][]messageHandler

func NewMessageHandlerList() MessageHandlerList {
	return MessageHandlerList{}
}

func (l MessageHandlerList) AddHandler(messageType string, handler messageHandler) {
	l[messageType] = append(l[messageType], handler)
}

func (l MessageHandlerList) Handle(decodedMessage KafkaDecodedMessage) error {
	handlers := l[decodedMessage.MessageType]
	if len(handlers) == 0 {
		return nil
	}

	errGroup := errgroup.Group{}

	for _, handler := range handlers {
		errGroup.Go(func() error {
			return handler.Handle(decodedMessage)
		})
	}

	return errGroup.Wait()
}

type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	consumer      consumer
}

func NewKafkaConsumer(
	kafkaBrokers []string,
	consumerGroupID string,
	handlerList MessageHandlerList,
	topics []string,
) (*KafkaConsumer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Version = sarama.V1_0_0_0
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, consumerGroupID, kafkaCfg)
	if err != nil {
		return nil, err
	}

	consumer := consumer{
		ready:       make(chan bool, 0),
		handlerList: handlerList,
		topics:      topics,
	}

	return &KafkaConsumer{
		consumer:      consumer,
		consumerGroup: consumerGroup,
	}, nil
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	ctx, ctxCancel := context.WithCancel(ctx)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			if err := c.consumerGroup.Consume(ctx, c.consumer.topics, &c.consumer); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.consumer.ready = make(chan bool, 0)
		}
	})

	<-c.consumer.ready

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
	case <-sigterm:
	}

	ctxCancel()
	if err := g.Wait(); err != nil {
		return err
	}

	return c.consumerGroup.Close()
}

type consumer struct {
	ready       chan bool
	handlerList MessageHandlerList
	topics      []string
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var decodedMessage KafkaDecodedMessage
		if err := json.Unmarshal(message.Value, &decodedMessage); err != nil {
			return err
		}

		_ = c.handlerList.Handle(decodedMessage)
		session.MarkMessage(message, "")
	}

	return nil
}

// Producer

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
