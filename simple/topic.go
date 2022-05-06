package simple

import (
	"context"
	"github.com/easy-bus/bus"
	"time"
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
	ensure func(context.Context, *bus.Message) bool) *bus.Handler {
	return Handler(queue, string(t), routeKey, handler, ensure)
}

func (t Topic) RunCommonHandler(
	routeKey, queue string,
	handler commonHandler,
	ensure commonEnsure) *bus.Handler {
	return RunCommonHandler(string(t), routeKey, queue, handler, ensure)
}

func (t Topic) RunCommonExHandler(
	routeKey, queue string,
	handler commonExHandler,
	ensure commonExEnsure) *bus.Handler {
	return RunCommonExHandler(string(t), routeKey, queue, handler, ensure)
}

func (t Topic) RunBatchExHandler(
	routeKey, queue string,
	handler batchExHandler,
	ensure batchExEnsure) *bus.Handler {
	return RunBatchExHandler(string(t), routeKey, queue, handler, ensure)
}

func (t Topic) RunSpecificHandler(
	routeKey, queue string,
	handler specificHandler,
	ensure specificEnsure) *bus.Handler {
	return RunSpecificHandler(string(t), routeKey, queue, handler, ensure)
}
