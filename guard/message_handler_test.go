package guard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageHandler(t *testing.T) {
	r := require.New(t)
	t.Run("Given an Interface that is not a function", func(t *testing.T) {
		messageHandler := struct{}{}
		t.Run("When guarded then panic is returned", func(t *testing.T) {
			r.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should be a func")
		})
	})
	t.Run("Given an Interface that is a function with a single input", func(t *testing.T) {
		messageHandler := func(struct{}) {}
		t.Run("When guarded then panic is returned", func(t *testing.T) {
			r.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should have two inputs")
		})
	})
	t.Run("Given an Interface that is a function with two input but the 1st is not a ctx", func(t *testing.T) {
		messageHandler := func(a, b struct{}) {}
		t.Run("When guarded then panic is returned", func(t *testing.T) {
			r.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler 1st input should be a ctx")
		})
	})
	t.Run("Given an Interface that is a function with two input but the 2nd is not a message struct", func(t *testing.T) {
		messageHandler := func(ctx context.Context, a func()) {}
		t.Run("When guarded then panic is returned", func(t *testing.T) {
			r.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler 2nd input should be a message struct")
		})
	})

	t.Run("Given an Interface that is a function with two input but it doesnt return error", func(t *testing.T) {
		messageHandler := func(ctx context.Context, a struct{}) string { return "" }
		t.Run("When guarded then panic is returned", func(t *testing.T) {
			r.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should return error")
		})
	})
}
