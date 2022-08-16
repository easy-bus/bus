package simple

import (
	"context"
	"time"

	"github.com/easy-bus/bus"
)

type Topic string

func (t Topic) Sender(
	ensure func(*bus.Message) bool,
	timeout time.Duration) *bus.Sender {
	return Sender(string(t), ensure, timeout)
}

func (t Topic) Handler(
	queue, routeKey string,
	handler func(context.Context, *bus.Message) error,
	ensure func(context.Context, *bus.Message) bool, opts ...bus.HandlerOpt) *bus.Handler {
	return Handler(queue, string(t), routeKey, handler, ensure, opts...)
}

func (t Topic) RunCommonHandler(
	routeKey, queue string,
	handler commonHandler,
	ensure commonEnsure, opts ...bus.HandlerOpt) *bus.Handler {
	return RunCommonHandler(string(t), routeKey, queue, handler, ensure, opts...)
}

func (t Topic) RunCommonExHandler(
	routeKey, queue string,
	handler commonExHandler,
	ensure commonExEnsure, opts ...bus.HandlerOpt) *bus.Handler {
	return RunCommonExHandler(string(t), routeKey, queue, handler, ensure, opts...)
}

func (t Topic) RunBatchExHandler(
	routeKey, queue string,
	handler batchExHandler,
	ensure batchExEnsure, opts ...bus.HandlerOpt) *bus.Handler {
	return RunBatchExHandler(string(t), routeKey, queue, handler, ensure, opts...)
}

func (t Topic) RunSpecificHandler(
	routeKey, queue string,
	handler specificHandler,
	ensure specificEnsure, opts ...bus.HandlerOpt) *bus.Handler {
	return RunSpecificHandler(string(t), routeKey, queue, handler, ensure, opts...)
}
