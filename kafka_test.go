package kfk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/wvanbergen/kazoo-go"
)

type sentTestingMessage struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestKafkaProduceAndConsume(t *testing.T) {
	assert := require.New(t)

	var (
		kafkaConsumer *KafkaConsumer
		kafkaProducer *KafkaProducer
		err           error
	)
	stChan := make(chan sentTestingMessage)
	topic := "topic-name"
	groupId := "group-id"

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	ctx, cancel := context.WithCancel(context.Background())

	setup := func() {

		messageHandler := NewHandler(func(ctx context.Context, s sentTestingMessage) error {
			stChan <- s
			return nil
		})

		kafkaConsumer, err = NewKafkaConsumer(
			[]string{kafkaBroker},
			groupId,
			[]string{topic},
		)
		assert.NoError(err)

		kafkaConsumer.AddHandler("sentTestingMessage", messageHandler)

		kafkaProducer, err = NewKafkaProducer([]string{kafkaBroker})
		assert.NoError(err)
	}

	tearDown := func() {
		cancel()
		newKazoo, _ := kazoo.NewKazoo([]string{"localhost:2181"}, nil)
		_ = newKazoo.DeleteTopicSync(topic, time.Second*5)
		_ = newKazoo.Consumergroup(groupId).Delete()
	}

	t.Run("Given a producer and a message is sent to kafka topic", func(t *testing.T) {
		setup()
		defer tearDown()

		msg := sentTestingMessage{
			ID:   "testing-message-id",
			Name: "testing-message-name",
		}
		err = kafkaProducer.Send(topic, msg.ID, msg)
		assert.NoError(err)

		t.Run("When the consumer is started", func(t *testing.T) {
			go func() {
				err = kafkaConsumer.Start(ctx)
				assert.NoError(err)
			}()

			t.Run("Then the message is consumed and the message information can be retrieved", func(t *testing.T) {
				message := <-stChan

				assert.Equal("testing-message-id", message.ID)
				assert.Equal("testing-message-name", message.Name)
			})
		})

		t.Run("When check", func(t *testing.T) {
			t.Run("Then is ok", func(t *testing.T) {
				check := kafkaProducer.HealthCheck(context.Background())
				assert.True(check)
			})
		})
	})
}

func TestKafkaFallbackConsume(t *testing.T) {
	assert := require.New(t)

	var (
		kafkaConsumer *KafkaConsumer
		kafkaProducer *KafkaProducer
		err           error
	)
	stChan := make(chan sentTestingMessage)
	topic := "topic-name-with-fallback"
	groupId := "group-id"

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	ctx, cancel := context.WithCancel(context.Background())

	setup := func() {

		messageHandler := func(ctx context.Context, s []byte) error {
			message := &sentTestingMessage{}
			if err := json.Unmarshal(s, message); err != nil {
				return err
			}
			stChan <- *message
			return nil
		}

		kafkaConsumer, err = NewKafkaConsumer(
			[]string{kafkaBroker},
			groupId,
			[]string{topic},
		)
		assert.NoError(err)

		kafkaConsumer.AddFallback(messageHandler)

		kafkaProducer, err = NewKafkaProducer([]string{kafkaBroker})
		assert.NoError(err)
	}

	tearDown := func() {
		newKazoo, _ := kazoo.NewKazoo([]string{"localhost:2181"}, nil)
		_ = newKazoo.DeleteTopicSync(topic, time.Second*5)
		_ = newKazoo.Consumergroup(groupId).Delete()
		cancel()
	}

	t.Run("Given a valid consumer and a message is sent to kafka topic", func(t *testing.T) {
		setup()
		defer tearDown()

		msg := sentTestingMessage{
			ID:   "testing-message-id",
			Name: "testing-message-name",
		}
		err = kafkaProducer.Send(topic, msg.ID, msg)
		assert.NoError(err)

		t.Run("When the consumer is started", func(t *testing.T) {
			go func() {
				err = kafkaConsumer.Start(ctx)
				assert.NoError(err)
			}()

			t.Run("Then the message is consumed by the fallback and the message information can be retrieved", func(t *testing.T) {
				message := <-stChan

				assert.Equal("testing-message-id", message.ID)
				assert.Equal("testing-message-name", message.Name)
			})
		})

		t.Run("When Checked then is ok", func(t *testing.T) {
			assert.True(kafkaConsumer.HealthCheck(context.Background()))
		})
	})
}

type CustomEncodingDecodingMessage struct {
	id   string
	name string
}

func (c *CustomEncodingDecodingMessage) UnmarshallKFK(data []byte) error {
	s := string(data)
	split := strings.Split(s, ";")
	c.id = split[0]
	c.name = split[1]
	return nil
}

func (c CustomEncodingDecodingMessage) MarshalKFK() ([]byte, error) {
	customEncoding := fmt.Sprintf("%s;%s", c.id, c.name)
	return []byte(customEncoding), nil
}

func TestCustomEncodeDecode(t *testing.T) {

	topic := "topic-name-with-custom-encoding"
	groupId := "group-id"

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	clusterAdmin, err := sarama.NewClusterAdmin([]string{kafkaBroker}, config)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	kafkaConsumer, err := NewKafkaConsumer(
		[]string{kafkaBroker},
		groupId,
		[]string{topic},
	)
	require.NoError(t, err)

	messageChan := make(chan CustomEncodingDecodingMessage)

	kafkaConsumer.AddHandler(
		"CustomEncodingDecodingMessage",
		NewHandler(func(ctx context.Context, message CustomEncodingDecodingMessage) error {
			cancel()
			go func() {
				messageChan <- message
			}()
			return nil
		}))

	kafkaProducer, err := NewKafkaProducer([]string{kafkaBroker})
	require.NoError(t, err)

	t.Run("Given a message with Marshall and Unmarshall", func(t *testing.T) {
		message := CustomEncodingDecodingMessage{
			id:   "testing-message-id",
			name: "testing-message-name",
		}
		t.Run("When send using producer", func(t *testing.T) {
			err := kafkaProducer.Send(topic, message.id, message)
			require.NoError(t, err)
			t.Run("Then is received with custom changes from the coding/encoding", func(t *testing.T) {
				err := kafkaConsumer.Start(ctx)
				require.NoError(t, err)
				require.Equal(t, message, <-messageChan)
			})
		})
		_ = clusterAdmin.DeleteTopic(topic)
		_ = clusterAdmin.DeleteConsumerGroup(groupId)
	})
}
