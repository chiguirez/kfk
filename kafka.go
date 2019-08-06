package kfk

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/sync/errgroup"
	"reflect"
	"sync"

	"github.com/Shopify/sarama"
)

// Consumer

type TopicMap map[string] MessageHandlerList

func (tm TopicMap) Topics() []string  {
	topics := reflect.ValueOf(tm).MapKeys()
	strings := make([]string,0, len(topics))
	for _,value := range topics {
		strings = append(strings, value.String())
	}
	return strings
}

type MessageHandlerList map[reflect.Type][]reflect.Value

func (l MessageHandlerList) Messages() []reflect.Type  {
	messages := reflect.ValueOf(l).MapKeys()
	values := make([]reflect.Type,0, len(messages))
	for _,value := range messages {
		values = append(values, value.Type())
	}
	return values
}


type messageHandler interface {}

func (l MessageHandlerList) AddHandler(message interface{}, handler messageHandler) {
	if isStruct(message) {
		panic("add MessageHandlerList requires an struct to decode messages into")
	}
	if isAFunc(handler) {
		panic("add MessageHandlerList requires a handler to be a function")
	}
	if funcHaveMoreThanOneAttrib(handler) || isAttributeEqualToMessage(handler, message) {
		errMsg := fmt.Sprintf("handler should only have one input parameter and must match %s",
			getMessageType(message).Name())
		panic(errMsg)
	}
	l[getMessageType(message)] = append(l[getMessageType(message)], reflect.ValueOf(handler))
}

func isAttributeEqualToMessage(handler messageHandler, message interface{}) bool {
	return reflect.TypeOf(handler).In(0) != getMessageType(message)
}

func getMessageType(message interface{}) reflect.Type {
	if reflect.TypeOf(message).Kind() == reflect.Struct{
		return reflect.TypeOf(message)
	}
	return reflect.TypeOf(message).Elem()
}

func funcHaveMoreThanOneAttrib(handler messageHandler) bool {
	return reflect.TypeOf(handler).NumIn() > 1
}

func isAFunc(handler messageHandler) bool {
	return reflect.TypeOf(handler).Kind() != reflect.Func
}

func isStruct(message interface{}) bool {
	return reflect.TypeOf(message).Kind() != reflect.Struct && (reflect.TypeOf(message).Kind() != reflect.Ptr || reflect.TypeOf(message).Elem().Kind() != reflect.Struct)
}


type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	consumer      consumer
}

func NewKafkaConsumer(
	kafkaBrokers []string,
	consumerGroupID string,
	topicMap TopicMap,
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
		ready:     make(chan bool, 0),
		topicMaps: topicMap,
	}

	return &KafkaConsumer{
		consumer:      consumer,		
		consumerGroup: consumerGroup,
	}, nil
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	defer c.consumerGroup.Close()
	topics := c.consumer.topicMaps.Topics()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if err := c.consumerGroup.Consume(ctx, topics, &c.consumer); err != nil {
					return err
				}
				c.consumer.ready = make(chan bool, 0)
			}
		}
	})

	return g.Wait()
}

type consumer struct {
	ready     chan bool
	topicMaps TopicMap
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
		messages := c.topicMaps[message.Topic].Messages()
		for _, value := range messages {
			messageType := reflect.New(value).Interface()
			if err := json.Unmarshal(message.Value, messageType); err != nil {
				continue
			}
			if reflect.DeepEqual(reflect.New(value).Interface(),messageType){
				continue
			}
			In := make([]reflect.Value, 0, 1)
			In = append(In, reflect.ValueOf(messageType))
			g := sync.WaitGroup{}
			g.Add(len(c.topicMaps[message.Topic][value]))
			for _, value := range c.topicMaps[message.Topic][value] {
				go func() {
					value.Call(In)
				}()
			}
		}

		defer session.MarkMessage(message, "")
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
