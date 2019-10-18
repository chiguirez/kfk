package kfk

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/Shopify/sarama"
	"github.com/chiguirez/kfk/guard"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

type MessageHandler struct {
	_type  reflect.Type
	_value reflect.Value
}

func NewHandler(handlerFunc interface{}) MessageHandler {
	guard.MessageHandler(handlerFunc)
	return MessageHandler{reflect.TypeOf(handlerFunc), reflect.ValueOf(handlerFunc)}
}

func (m MessageHandler) Handle(ctx context.Context, msg []byte) error {
	value := reflect.New(m._type.In(1)).Interface()
	if err := json.Unmarshal(msg, value); err != nil {
		return err
	}
	res := m._value.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(value).Elem()})
	err, ok := res[0].Interface().(error)
	if ok {
		return err
	}
	return nil
}

type messageHandlerList map[string][]MessageHandler

func (l messageHandlerList) AddHandler(messageType string, handler MessageHandler) {
	l[messageType] = append(l[messageType], handler)
}

func (l messageHandlerList) Handle(ctx context.Context, message []byte) error {
	handlers, ok := l[gjson.ParseBytes(message).Get("@type").String()]
	if len(handlers) == 0 || !ok {
		return nil
	}
	g, _ := errgroup.WithContext(ctx)

	for _, handler := range handlers {
		g.Go(func() error {
			return handler.Handle(ctx, message)
		})
	}

	return g.Wait()
}

type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	consumer      consumer
}

func NewKafkaConsumer(
	kafkaBrokers []string,
	consumerGroupID string,
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
	ready       chan bool
	handlerList messageHandlerList
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
		_ = c.handlerList.Handle(session.Context(), message.Value)
		session.MarkMessage(message, "")
	}
	return nil
}
