package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/Shopify/sarama"
	"golang.org/x/sync/errgroup"

	"github.com/chiguirez/kfk/v3/consumer/handler/guard"
)

type Handler struct {
	_type  reflect.Type
	_value reflect.Value
}

type Unmarshaller interface {
	UnmarshallKFK([]byte) error
}

func (m Handler) decode(msg []byte, value interface{}) error {
	unmarshaller, ok := value.(Unmarshaller)
	if ok {
		return unmarshaller.UnmarshallKFK(msg)
	}

	return json.Unmarshal(msg, value)
}

func New(handlerFunc interface{}) Handler {
	guard.MessageHandler(handlerFunc)

	return Handler{reflect.TypeOf(handlerFunc), reflect.ValueOf(handlerFunc)}
}

func (m Handler) Handle(ctx context.Context, msg []byte) error {
	value := reflect.New(m._type.In(1)).Interface()
	if err := m.decode(msg, value); err != nil {
		return err
	}

	res := m._value.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(value).Elem()})

	err, ok := res[0].Interface().(error)
	if ok {
		return err
	}

	return nil
}

type List map[string][]Handler

var ErrNotFound = errors.New("handler not found")

func (l List) Handle(ctx context.Context, message *sarama.ConsumerMessage) error {
	headerType := getTypeFromHeader(message)

	handlers, ok := l[headerType]
	if len(handlers) == 0 || !ok {
		return fmt.Errorf("%w for message %s", ErrNotFound, headerType)
	}

	g, _ := errgroup.WithContext(ctx)

	for _, handler := range handlers {
		g.Go(
			func(handler Handler) func() error {
				return func() error {
					return handler.Handle(ctx, message.Value)
				}
			}(handler),
		)
	}

	return g.Wait()
}

func (l List) AddHandler(messageType string, handler Handler) {
	l[messageType] = append(l[messageType], handler)
}

func getTypeFromHeader(message *sarama.ConsumerMessage) string {
	for _, h := range message.Headers {
		if string(h.Key) == "@type" {
			return string(h.Value)
		}
	}

	return ""
}
