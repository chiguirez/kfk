package kfk_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/suite"

	"github.com/chiguirez/kfk/v3/consumer"
	"github.com/chiguirez/kfk/v3/consumer/handler"
	"github.com/chiguirez/kfk/v3/producer"
)

type kfkSuite struct {
	suite.Suite
	sut struct {
		consumer *consumer.Consumer
		producer *producer.Producer
	}
	cancelFn     context.CancelFunc
	topics       []string
	consumerChan chan consumedMsg
}

type consumedMsg struct {
	msg   *testingMessage
	topic string
}

func (k *kfkSuite) TearDownSuite() {
	k.cancelFn()

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = broker
	}

	config := sarama.NewConfig()
	config.Version = sarama.V1_1_0_0

	admin, err := sarama.NewClusterAdmin([]string{kafkaBroker}, config)
	k.Require().NoError(err)

	err = admin.DeleteTopic(k.topics[0])
	k.Require().NoError(err)
}

func (k *kfkSuite) SetupSuite() {
	k.topics = []string{"topic_test"}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = broker
	}

	var (
		ctx context.Context
		err error
	)

	ctx, k.cancelFn = context.WithCancel(context.Background())

	k.consumerChan = make(chan consumedMsg, 1)

	messageHandler := handler.New(func(ctx context.Context, s *testingMessage) error {
		topic, _ := consumer.TopicFromContext(ctx)
		msg := consumedMsg{
			msg:   s,
			topic: topic,
		}

		k.consumerChan <- msg

		return nil
	})

	k.sut.consumer, err = consumer.New(
		[]string{kafkaBroker},
		groupID,
		k.topics,
	)
	k.Require().NoError(err)

	k.sut.consumer.AddHandler("testingMessage", messageHandler)

	go func() {
		_ = k.sut.consumer.Start(ctx)
	}()

	k.sut.producer, err = producer.New([]string{kafkaBroker})
	k.Require().NoError(err)
}

const (
	broker  = "localhost:9092"
	groupID = "group-id"
)

type testingMessage struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestKfk(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(kfkSuite))
}

func (k *kfkSuite) TestProducer() {
	k.Run("Given a message", func() {
		message := &testingMessage{
			ID:   "ID",
			Name: "Name",
		}
		k.Run("When dispatched in the producer", func() {
			err := k.sut.producer.Send(k.topics[0], message.ID, message)
			k.Run("Then no errors from producer and it can be consumed from topic", func() {
				k.Require().NoError(err)

				msg := <-k.consumerChan

				k.Require().Equal(*message, *(msg.msg))
				k.Require().Equal(k.topics[0], msg.topic)
			})
		})
	})
}

func (k *kfkSuite) TestCheck() {
	k.Run("Given a Producer", func() {
		kfkProducer := k.sut.producer
		k.Run("When checked", func() {
			check := kfkProducer.HealthCheck(context.Background())
			k.Run("Then is ok", func() {
				k.Require().True(check)
			})
		})
	})

	k.Run("Given a Consumer", func() {
		kfkConsumer := k.sut.consumer
		k.Run("When checked", func() {
			check := kfkConsumer.HealthCheck(context.Background())
			k.Run("Then is ok", func() {
				k.Require().True(check)
			})
		})
	})
}

func (k *kfkSuite) TestFallback() {
	k.sut.consumer.AddFallback(func(ctx context.Context, bytes []byte) error {
		topic, _ := consumer.TopicFromContext(ctx)
		k.consumerChan <- consumedMsg{topic: topic}

		return nil
	})

	k.Run("Given a message not handled", func() {
		message := struct{ A bool }{A: true}
		k.Run("When a fallback exist", func() {
			_ = k.sut.producer.Send(k.topics[0], "key", message)
			k.Run("Then fallback function catch it", func() {
				msg := <-k.consumerChan

				k.Require().Equal(k.topics[0], msg.topic)
			})
		})
	})
}

type customEncodingDecodingMessage struct {
	id   string
	name string
}

func (c *customEncodingDecodingMessage) UnmarshallKFK(data []byte) error {
	s := string(data)
	split := strings.Split(s, ";")
	c.id = split[0]
	c.name = split[1]

	return nil
}

func (c customEncodingDecodingMessage) MarshalKFK() ([]byte, error) {
	customEncoding := fmt.Sprintf("%s;%s", c.id, c.name)

	return []byte(customEncoding), nil
}

func (k *kfkSuite) TestCustomEncodeDecode() {
	customChan := make(chan customEncodingDecodingMessage, 1)

	k.sut.consumer.AddHandler("customEncodingDecodingMessage", handler.New(func(ctx context.Context, message customEncodingDecodingMessage) error {
		customChan <- message

		return nil
	}))

	k.Run("Given a message with Marshall and Unmarshall", func() {
		message := customEncodingDecodingMessage{
			id:   "testing-message-id",
			name: "testing-message-name",
		}
		k.Run("When send using producer", func() {
			err := k.sut.producer.Send(k.topics[0], message.id, message)

			k.Require().NoError(err)
			k.Run("Then is received with custom changes from the coding/encoding", func() {
				msg := <-customChan

				k.Require().Equal(message, msg)
			})
		})
	})
}
