package guard_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	. "github.com/chiguirez/kfk/v3/consumer/handler/guard"
)

type guardSuite struct {
	suite.Suite
}

func (g *guardSuite) TestGuardNotAFunction() {
	g.Run("Given an Interface that is not a function", func() {
		messageHandler := struct{}{}
		g.Run("When guarded then panic is returned", func() {
			g.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should be a func")
		})
	})
}

func (g *guardSuite) TestGuardSingleInput() {
	g.Run("Given an Interface that is a function with a single input", func() {
		messageHandler := func(struct{}) {}
		g.Run("When guarded then panic is returned", func() {
			g.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should have two inputs")
		})
	})
}

func (g *guardSuite) TestGuardNotACtx() {
	g.Run("Given an Interface that is a function with two input but the 1st is not a ctx", func() {
		messageHandler := func(a, b struct{}) {}
		g.Run("When guarded then panic is returned", func() {
			g.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler 1st input should be a ctx")
		})
	})
}

func (g *guardSuite) TestGuardNotAStruct() {
	g.Run("Given an Interface that is a function with two input but the 2nd is not a message struct", func() {
		messageHandler := func(ctx context.Context, a func()) {}
		g.Run("When guarded then panic is returned", func() {
			g.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler 2nd input should be a message struct")
		})
	})
}

func (g *guardSuite) TestGuardNotReturnsError() {
	g.Run("Given an Interface that is a function with two input but it doesnt return error", func() {
		messageHandler := func(ctx context.Context, a struct{}) string { return "" }
		g.Run("When guarded then panic is returned", func() {
			g.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should return error")
		})
	})
}

func (g *guardSuite) TestGuardNotAStructError() {
	g.Run("Given an Interface that is a function with two input but is not a struct", func() {
		messageHandler := func(ctx context.Context, a bool) error { return nil }
		g.Run("When guarded then panic is returned", func() {
			g.Panics(func() {
				MessageHandler(messageHandler)
			}, "handler should return error")
		})
	})
}

func (g *guardSuite) TestSuccess() {
	g.Run("Given an Interface that is a function with two input return error", func() {
		messageHandler := func(ctx context.Context, a *struct{}) error { return nil }
		g.Run("When guarded then panic is returned", func() {
			g.NotPanics(func() {
				MessageHandler(messageHandler)
			})
		})
	})
}

func TestMessageHandler(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(guardSuite))
}
