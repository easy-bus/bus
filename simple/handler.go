package simple

import (
	"context"
	"fmt"
	"github.com/easy-bus/bus"
	"time"
)

func Handler(
	queue, topic, routeKey string,
	handler func(context.Context, *bus.Message) error,
	ensure func(context.Context, *bus.Message) bool,
) *bus.Handler {
	ctx := cancelGroup.newCtx()
	return &bus.Handler{
		Context: ctx,
		Queue:   queue,
		Subscribe: bus.Subscribe{
			Topic:    topic,
			RouteKey: routeKey,
		},
		Driver:     driver,
		Logger:     logger,
		DLStorage:  dlStorage,
		Idempotent: idempotent,
		HandleFunc: func(msg *bus.Message) bool {
			return handler(ctx, msg) == nil
		},
		EnsureFunc: func(msg *bus.Message) bool {
			return ensure == nil || ensure(ctx, msg)
		},
		RetryDelay: func(attempts int) time.Duration {
			return time.Duration(1 - attempts) // 立即重试且重试一次
		},
	}
}

// Common
type commonEnsure func(ctx context.Context, id string) bool
type commonHandler func(ctx context.Context, id string) error

func runCommonHandler(topic, routeKey, queue string, handler commonHandler, ensure commonEnsure) *bus.Handler {
	h := Handler(
		fmt.Sprintf("%s.%s", topic, queue), topic, routeKey,
		func(ctx context.Context, message *bus.Message) error {
			return handler(ctx, LoadCommon(message).ID)
		},
		func(ctx context.Context, message *bus.Message) bool {
			return ensure == nil || ensure(ctx, LoadCommon(message).ID)
		},
	)
	return handlerGroup.add(h)
}

// CommonEX
type commonExEnsure func(ctx context.Context, id string, ex Extend) bool
type commonExHandler func(ctx context.Context, id string, ex Extend) error

func runCommonExHandler(topic, routeKey, queue string, handler commonExHandler, ensure commonExEnsure) *bus.Handler {
	h := Handler(
		fmt.Sprintf("%s.%s", topic, queue), topic, routeKey,
		func(ctx context.Context, message *bus.Message) error {
			var evt = LoadCommonEx(message)
			return handler(ctx, evt.ID, evt.EX)
		},
		func(ctx context.Context, message *bus.Message) bool {
			var evt = LoadCommonEx(message)
			return ensure == nil || ensure(ctx, evt.ID, evt.EX)
		},
	)
	return handlerGroup.add(h)
}

// BatchEX
type batchExEnsure func(ctx context.Context, id []string, ex Extend) bool
type batchExHandler func(ctx context.Context, id []string, ex Extend) error

func runBatchExHandler(topic, routeKey, queue string, handler batchExHandler, ensure batchExEnsure) *bus.Handler {
	h := Handler(
		fmt.Sprintf("%s.%s", topic, queue), topic, routeKey,
		func(ctx context.Context, message *bus.Message) error {
			var evt = LoadBatchEx(message)
			return handler(ctx, evt.IDS, evt.EX)
		},
		func(ctx context.Context, message *bus.Message) bool {
			var evt = LoadBatchEx(message)
			return ensure == nil || ensure(ctx, evt.IDS, evt.EX)
		},
	)
	return handlerGroup.add(h)
}

// 自定义事件结构体
type specificEnsure func(ctx context.Context, message *bus.Message) bool
type specificHandler func(ctx context.Context, message *bus.Message) error

func runSpecificHandler(topic, routeKey, queue string, handler specificHandler, ensure specificEnsure) *bus.Handler {
	h := Handler(
		fmt.Sprintf("%s.%s", topic, queue), topic, routeKey,
		func(ctx context.Context, message *bus.Message) error {
			return handler(ctx, message)
		},
		func(ctx context.Context, message *bus.Message) bool {
			return ensure == nil || ensure(ctx, message)
		},
	)
	return handlerGroup.add(h)
}
