package simple

import (
	"time"

	"github.com/easy-bus/bus"
)

func Sender(topic string, ensure func(*bus.Message) bool, timeout time.Duration) *bus.Sender {
	s := &bus.Sender{
		Topic:  topic,
		Driver: driver,
		Logger: logger,
		TxOptions: &bus.TxOptions{
			Context:   cancelGroup.newCtx(),
			TxStorage: txStorage,
			Timeout:   timeout,
			EnsureFunc: func(msg *bus.Message) bool {
				return ensure == nil || ensure(msg)
			},
			RetryDelay: func(attempts int) time.Duration {
				return time.Duration(1 - attempts) // 立即重试且重试一次
			},
		},
	}
	return senderGroup.add(s.Prepare())
}
