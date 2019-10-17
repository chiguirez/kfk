package kfk

import (
	"context"
	"os"
	"testing"
	"time"

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
		newKazoo, _ := kazoo.NewKazoo([]string{"localhost:2181"}, nil)
		_ = newKazoo.DeleteTopicSync(topic, time.Second*5)
		_ = newKazoo.Consumergroup(groupId).Delete()
	}

	t.Run("Given a message is sent to kafka topic", func(t *testing.T) {
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
				err = kafkaConsumer.Start(context.Background())
				assert.NoError(err)
			}()

			t.Run("Then the message is consumed and the message information can be retrieved", func(t *testing.T) {
				message := <-stChan

				assert.Equal("testing-message-id", message.ID)
				assert.Equal("testing-message-name", message.Name)
			})
		})
	})
}
