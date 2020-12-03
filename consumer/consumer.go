package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"

	"github.com/chiguirez/kfk/v2/consumer/handler"
)

type Consumer struct {
	consumerGroup sarama.ConsumerGroup
	consumer      saramaInternalConsumer
	client        sarama.Client
}

var errCreateConsumer = fmt.Errorf("error during consumer creation")

func New(kafkaBrokers []string, consumerGroupID string, topics []string, cfgOptions ...CfgOption) (*Consumer, error) {
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Return.Errors = true
	kafkaCfg.Version = sarama.V1_1_0_0
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	for _, opt := range cfgOptions {
		opt(kafkaCfg)
	}

	client, err := sarama.NewClient(kafkaBrokers, kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCreateConsumer, err)
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(consumerGroupID, client)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCreateConsumer, err)
	}

	consumer := saramaInternalConsumer{
		ready:       make(chan bool),
		handlerList: handler.List{},
		topics:      topics,
	}

	return &Consumer{
		consumer:      consumer,
		consumerGroup: consumerGroup,
		client:        client,
	}, nil
}

func (c *Consumer) AddHandler(messageType string, handler handler.Handler) {
	c.consumer.handlerList.AddHandler(messageType, handler)
}

func (c *Consumer) AddFallback(fn FallbackFunc) {
	c.consumer.fallbackHandler = fn
}

var errConsuming = fmt.Errorf("error during consuming of messages")

func (c *Consumer) Start(ctx context.Context) error {
	defer func() {
		_ = c.consumerGroup.Close()
	}()

	for {
		select {
		case err := <-c.consumerGroup.Errors():
			return fmt.Errorf("%w: %v", errConsuming, err)
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err()
			}

			return nil
		default:
			if err := c.consumerGroup.Consume(ctx, c.consumer.topics, &c.consumer); err != nil {
				return fmt.Errorf("%w: %v", errConsuming, err)
			}

			c.consumer.ready = make(chan bool)
		}
	}
}

func (c *Consumer) HealthCheck(_ context.Context) bool {
	controller, err := c.client.Controller()
	if err != nil {
		return false
	}

	connected, err := controller.Connected()
	if err != nil {
		return false
	}

	return connected
}

type FallbackFunc func(context.Context, []byte) error

type CfgOption func(config *sarama.Config)

func FromNewest() CfgOption {
	return func(config *sarama.Config) {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
}

type saramaInternalConsumer struct {
	ready           chan bool
	handlerList     handler.List
	topics          []string
	fallbackHandler FallbackFunc
}

func (c *saramaInternalConsumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)

	return nil
}

func (c *saramaInternalConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

type contextKey string

func TopicFromContext(ctx context.Context) (string, bool) {
	ContextTopic := contextKey("Topic")

	topic, ok := ctx.Value(ContextTopic).(string)

	return topic, ok
}

func (c *saramaInternalConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ContextTopic := contextKey("Topic")

	for message := range claim.Messages() {
		select {
		case <-session.Context().Done():
			return nil
		default:
			ctx := context.WithValue(session.Context(), ContextTopic, message.Topic)

			err := c.handlerList.Handle(ctx, message)
			if errors.Is(err, handler.ErrNotFound) && c.fallbackHandler != nil {
				_ = c.fallbackHandler(ctx, message.Value)
			}

			session.MarkMessage(message, "")
		}
	}

	return nil
}
