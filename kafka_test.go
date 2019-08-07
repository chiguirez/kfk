package kfk

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKafkaConsumer_MessageHandlerList(t *testing.T) {
	r := require.New(t)
	lists := MessageHandlerList{}
	t.Run("Given a valid struct and a valid function when added as handler then it doesn't panic", func(t *testing.T) {
		r.NotPanics(func() {
			lists.AddHandler(t, func(testing.T) {})
		})
	})
	t.Run("Given an invalid struct and a valid function when added as handler then it should panics", func(t *testing.T) {
		r.Panics(func() {
			lists.AddHandler(func(){}, func(testing.T) {})
		})
	})
	t.Run("Given an valid struct and a invalid function when added as handler then it should panics", func(t *testing.T) {
		r.Panics(func() {
			lists.AddHandler(t, t)
		})
	})
	t.Run("Given a valid struct and a valid function but with more attributes when added as handler then it should panic", func(t *testing.T) {
		r.Panics(func() {
			lists.AddHandler(t, func(testing.T,testing.B) {})
		})
	})
	t.Run("Given a valid struct and a valid function but attributes doesnt match struct when added as handler then it should panic", func(t *testing.T) {
		r.Panics(func() {
			lists.AddHandler(t, func(testing.B) {})
		})
	})


	t.Run("Given a valid MessageHandlerList", func(t *testing.T) {
		lists := MessageHandlerList{}
		lists.AddHandler(t, func(testing.T) {})
		lists.AddHandler(testing.B{}, func(testing.B) {})
		t.Run("When Messages method is called then it should return as many as MessageHandler added to the list", func(t *testing.T) {
			r.Len(lists.Messages(),2)
		})
	})
}

type A struct{
	ID int `json:"id"`
	Name string `json:"name"`
	Time time.Time `json:"time"`
}

func TestKafkaConsumer_ConsumeClaim(t *testing.T){
	r := require.New(t)

	t.Run("Given a message struct A", func(t *testing.T) {
		message := A{
			ID:   1,
			Name: "Name",
			Time: time.Now(),
		}
		t.Run("When Produced to Kafka test_topic", func(t *testing.T) {
			producer, err := NewKafkaProducer([]string{":9092"})
			r.NoError(err)
			err = producer.Send("test_topic", "", message)
			r.NoError(err)

			t.Run("Then it can be fetch with KfkConsumer", func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				maps := SchemaRegistry{}
				lists := MessageHandlerList{}
				lists.AddHandler(new(A), func(a A) {r.NotEmpty(a); cancel()})
				maps.AddTopic("test_topic", lists)
				kafkaConsumer, err := NewKafkaConsumer([]string{":9092"}, "test", maps)
				r.NoError(err)
				err = kafkaConsumer.Start(ctx)
				r.NoError(err)
			})
		})
	})


}