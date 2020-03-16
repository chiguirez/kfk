package kfk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/Shopify/sarama"
	"github.com/chiguirez/kfk/v2/guard"
	"golang.org/x/sync/errgroup"
)

type MessageHandler struct {
	_type  reflect.Type
	_value reflect.Value
}

type FallbackFunc func(context.Context, []byte) error

func NewHandler(handlerFunc interface{}) MessageHandler {
	guard.MessageHandler(handlerFunc)
	return MessageHandler{reflect.TypeOf(handlerFunc), reflect.ValueOf(handlerFunc)}
}

func (m MessageHandler) Handle(ctx context.Context, msg []byte) error {
	value := reflect.New(m._type.In(1)).Interface()
	if err := m.decode(msg, value); err != nil {
		return err
	}
	res := m._value.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(value).Elem()})
	err, ok := res[0].Interface().(error)
	if ok {
		return err
	}
	return nil
}

type Unmarshaler interface {
	UnmarshalKFK([]byte) error
}

func (m MessageHandler) decode(msg []byte, value interface{}) error {
	unmarshaler, ok := value.(Unmarshaler)
	if ok {
		return unmarshaler.UnmarshalKFK(msg)
	}
	return json.Unmarshal(msg, value)
}

type messageHandlerList map[string][]MessageHandler

func (l messageHandlerList) AddHandler(messageType string, handler MessageHandler) {
	l[messageType] = append(l[messageType], handler)
}

var handlerNotFound = errors.New("handler not found")

func (l messageHandlerList) Handle(ctx context.Context, message *sarama.ConsumerMessage) error {
	headerType := getTypeFromHeader(message)
	handlers, ok := l[headerType]
	if len(handlers) == 0 || !ok {
		return fmt.Errorf("%w for message %s", handlerNotFound, headerType)
	}
	g, _ := errgroup.WithContext(ctx)

	for _, handler := range handlers {
		g.Go(func() error {
			return handler.Handle(ctx, message.Value)
		})
	}

	return g.Wait()
}

type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	consumer      consumer
}

type ConsumerCfgOption func(config *sarama.Config)

func FromNewest() ConsumerCfgOption {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
}

func NewKafkaConsumer(
	kafkaBrokers []string,
	consumerGroupID string,
	topics []string,
	cfgOptions ...ConsumerCfgOption,
) (*KafkaConsumer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Version = sarama.V1_0_0_0
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	for _, opt := range cfgOptions {
		opt(kafkaCfg)
	}

	consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, consumerGroupID, kafkaCfg)
	if err != nil {
		return nil, err
	}

	consumer := consumer{
		ready:       make(chan bool),
		handlerList: messageHandlerList{},
		topics:      topics,
	}

	return &KafkaConsumer{
		consumer:      consumer,
		consumerGroup: consumerGroup,
	}, nil
}

func (c *KafkaConsumer) AddHandler(messageType string, handler MessageHandler) {
	c.consumer.handlerList.AddHandler(messageType, handler)
}

func (c *KafkaConsumer) AddFallback(fn FallbackFunc) {
	c.consumer.fallbackHandler = fn
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	defer func() {
		_ = c.consumerGroup.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() != context.Canceled {
				return ctx.Err()
			}
			return nil
		default:
			if err := c.consumerGroup.Consume(ctx, c.consumer.topics, &c.consumer); err != nil {
				return err
			}
			c.consumer.ready = make(chan bool)
		}
	}
}

type consumer struct {
	ready           chan bool
	handlerList     messageHandlerList
	topics          []string
	fallbackHandler FallbackFunc
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
		err := c.handlerList.Handle(session.Context(), message)
		if errors.Is(err, handlerNotFound) && c.fallbackHandler != nil {
			_ = c.fallbackHandler(session.Context(), message.Value)
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func getTypeFromHeader(message *sarama.ConsumerMessage) string {
	for _, h := range message.Headers {
		if string(h.Key) == "@type" {
			return string(h.Value)
		}
	}
	return ""
}
