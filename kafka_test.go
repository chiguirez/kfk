package kfk

import (
	"github.com/stretchr/testify/require"
	"testing"
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