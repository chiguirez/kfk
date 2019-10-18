package guard

import (
	"context"
	"reflect"
)

func HaveTwoAttributes(handler interface{}) bool {
	return reflect.TypeOf(handler).NumIn() == 2
}

func IsACtx(handler interface{}, attributePosition int) bool {
	ctxInterface := reflect.TypeOf((*context.Context)(nil)).Elem()
	return reflect.TypeOf(handler).In(attributePosition).Kind() == reflect.Interface &&
		reflect.TypeOf(handler).In(attributePosition).Implements(ctxInterface)
}

func IsAStruct(handler interface{}, attributePosition int) bool {
	return reflect.TypeOf(handler).In(attributePosition).Kind() == reflect.Struct
}

func IsAFunc(handler interface{}) bool {
	return reflect.TypeOf(handler).Kind() == reflect.Func
}

func FuncReturnError(handler interface{}) bool {
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	return reflect.TypeOf(handler).Out(0).Implements(errorInterface)
}

func MessageHandler(handlerFunc interface{}) {
	if !IsAFunc(handlerFunc) {
		panic("handler should be a func")
	}
	if !HaveTwoAttributes(handlerFunc) {
		panic("handler input params should be a ctx and a message")
	}
	if !IsACtx(handlerFunc, 0) {
		panic("first attribute of the handler should be a context")
	}
	if !IsAStruct(handlerFunc, 1) {
		panic("second attribute of the handler should be a struct message")
	}
	if !FuncReturnError(handlerFunc) {
		panic("handler output should be of type error")
	}
}
