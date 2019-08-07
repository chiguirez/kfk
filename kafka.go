package kfk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"reflect"
	"sync"

	"github.com/Shopify/sarama"
)




type KafkaConsumer struct {
	consumerGroup sarama.ConsumerGroup
	consumer      consumer
}

func NewKafkaConsumer(
	kafkaBrokers []string,
	consumerGroupID string,
	schemaRegistries SchemaRegistry,
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
		schemaRegistries: schemaRegistries,
	}

	return &KafkaConsumer{
		consumer:      consumer,		
		consumerGroup: consumerGroup,
	}, nil
}

func (c *KafkaConsumer) Start(ctx context.Context) error {
	defer c.consumerGroup.Close()
	topics := c.consumer.schemaRegistries.Topics()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				if ctx.Err() != context.Canceled{
					return ctx.Err()
				}
				return nil
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
	schemaRegistries SchemaRegistry
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
		messages := c.schemaRegistries[message.Topic].Messages()
		for _, value := range messages {
			base := reflect.New(value).Interface()

			if err := json.Unmarshal(message.Value, base); err != nil {
				continue
			}
			if reflect.DeepEqual(reflect.New(value).Interface(),base){
				continue
			}
			In := make([]reflect.Value, 0, 1)
			In = append(In, reflect.ValueOf(base).Elem())
			g := sync.WaitGroup{}
			g.Add(len(c.schemaRegistries[message.Topic][value]))
			for _, value := range c.schemaRegistries[message.Topic][value] {
				go func() {
					value.Call(In)
					g.Done()
				}()
			}
			g.Wait()
			session.MarkMessage(message, "")
		}
	}
	return nil
}

type SchemaRegistry map[string]MessageHandlerList

func (sr SchemaRegistry) AddTopic(topic string, list MessageHandlerList){
	sr[topic] = list
}

func (sr SchemaRegistry) Topics() []string  {
	topics := reflect.ValueOf(sr).MapKeys()
	strings := make([]string,0, len(topics))
	for _,value := range topics {
		strings = append(strings, value.String())
	}
	return strings
}

type MessageHandlerList map[reflect.Type][]reflect.Value

func (l MessageHandlerList) Messages() []reflect.Type  {
	values := make([]reflect.Type,0, len(l))
	for key,_ := range l {
		values = append(values, key)
	}
	return values
}


type messageHandler interface {}

func (l MessageHandlerList) AddHandler(message interface{}, handler messageHandler) {
	if !isStruct(message) {
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
	return reflect.TypeOf(message).Kind() == reflect.Struct || reflect.TypeOf(message).Kind() == reflect.Ptr && reflect.TypeOf(message).Elem().Kind() == reflect.Struct
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


func (p *KafkaProducer) Send(topic string, id string, message interface{}) error {

	if ! isStruct(message){
		return errors.New("send message is required to be an struct or ptr to struct")
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Key: sarama.ByteEncoder(id),
		Value:   sarama.ByteEncoder(bytes),
	}
	_, _, err = p.producer.SendMessage(producerMessage)

	return err
}