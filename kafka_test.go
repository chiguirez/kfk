package kfk

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/require"
	"github.com/wvanbergen/kazoo-go"
)

type sentTestingMessage struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}

func (m sentTestingMessage) ID() string {
	return m.Id
}

func TestKafkaProduceAndConsume(t *testing.T) {
	assert := require.New(t)

	var (
		messageHandler *messageHandlerMock
		kafkaConsumer  *KafkaConsumer
		kafkaProducer  *KafkaProducer
		err            error
	)

	decodedMessageChan := make(chan KafkaDecodedMessage, 0)

	messageHandlerList := MessageHandlerList{}
	topic := "topic-name"
	groupId := "group-id"

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	setup := func() {
		messageHandler = &messageHandlerMock{
			HandleFunc: func(in1 KafkaDecodedMessage) error {
				decodedMessageChan <- in1
				return nil
			},
		}

		messageHandlerList.AddHandler("sentTestingMessage", messageHandler)

		kafkaConsumer, err = NewKafkaConsumer(
			[]string{kafkaBroker},
			groupId,
			messageHandlerList,
			[]string{topic},
		)
		assert.NoError(err)

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
			Id:   "testing-message-id",
			Name: "testing-message-name",
		}
		err = kafkaProducer.Send(topic, msg)
		assert.NoError(err)

		t.Run("When the consumer is started", func(t *testing.T) {
			go func() {
				err = kafkaConsumer.Start(context.Background())
				assert.NoError(err)
			}()

			t.Run("Then the message is consumed", func(t *testing.T) {
				message := <-decodedMessageChan
				assert.True(len(messageHandler.HandleCalls()) > 0)

				t.Run("And the message information can be retrieved", func(t *testing.T) {
					var sent sentTestingMessage
					err = mapstructure.Decode(message.Body, &sent)
					assert.NoError(err)

					assert.Equal("testing-message-id", sent.Id)
					assert.Equal("testing-message-name", sent.Name)
					assert.Equal("sentTestingMessage", message.MessageType)
				})
			})
		})
	})
}
