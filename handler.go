package bus

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/letsfire/utils"
)

// Subscribe 处理器订阅
type Subscribe struct {
	// Topic 订阅主题
	Topic string

	// RouteKey 路由键
	RouteKey string
}

// Handler 消息处理器
type Handler struct {
	// Queue 处理队列的名称
	Queue string

	// Delay 消息处理延迟时长
	Delay time.Duration

	// Driver 驱动实例
	Driver DriverInterface

	// Subscribe 订阅配置
	Subscribe Subscribe

	// DLStorage 死信存储
	// 无法处理的消息最终流转到这里
	DLStorage DLStorageInterface

	// ErrorFunc 异常错误处理
	// 通常用于记录日志并上报通知
	ErrorFunc func(err error)

	// HandleFunc 消息处理回调函数
	// 若返回值为true则表示处理成功, 将删除该消息
	// 若返回值为false则表示处理失败, 消息将延迟重试
	HandleFunc func(msg *Message) (done bool)

	// Idempotent 幂等判断实现
	// 防止消息被重复处理保证数据一致性
	// 若幂等性判断自身异常则可能导致判断失效
	// 因此再严格一致的场景下配置EnsureFn进行二次确认
	Idempotent IdempotentInterface

	// EnsureFunc 幂等性的二次确认
	// 请一定要注意布尔返回值的代表含义
	// 若返回值为true表示未处理, 即允许处理
	// 若返回值为false表示已处理, 即不允许处理
	// 通常情况下并不会执行, 仅当Idempotent出现异常时起作用
	// 若使用场景不严格要求数据一致的可以不用配置
	EnsureFunc func(msg *Message) (allow bool)

	// RetryDelay 重试延迟机制
	// 返回值为重试间隔, 若 < 0 则代表不进行重试
	RetryDelay func(attempts int) time.Duration

	// ready 是否就绪
	ready bool
}

// Prepare 准备就绪
func (h *Handler) Prepare() *Handler {
	if h.ready {
		return h
	}
	if h.Queue == "" {
		panic("easy-bus: the handler missing queue name")
	}
	if h.Driver == nil {
		panic(fmt.Sprintf("easy-bus: the handler %q missing driver instance", h.Queue))
	}
	if h.HandleFunc == nil {
		panic(fmt.Sprintf("easy-bus: the handler %q missing handle function", h.Queue))
	}
	if h.DLStorage == nil {
		h.DLStorage = nullDLStorage{}
	}
	if h.Idempotent == nil {
		h.Idempotent = nullIdempotent{}
	}
	if h.RetryDelay == nil {
		h.RetryDelay = func(int) time.Duration { return -1 }
	}
	if err := h.Driver.CreateQueue(h.Queue, h.Delay); err != nil {
		panic(fmt.Sprintf("easy-bus: then handler %q create queue error, %v", h.Queue, err))
	}
	if h.Subscribe.Topic != "" {
		utils.Must(h.Driver.Subscribe(h.Subscribe.Topic, h.Queue, h.Subscribe.RouteKey))
	}
	h.ready = true
	return h
}

// Run 启动处理器
func (h *Handler) Run() {
	h.RunCtx(context.Background())
}

// RunCtx 启动处理器
func (h *Handler) RunCtx(ctx context.Context) {
	if h.ready == false {
		panic(fmt.Sprintf("easy-bus: run is forbidden when the handler %q has not prepared", h.Queue))
	}
	errChan := make(chan error)
	utils.Goroutine(func() {
		for err := range errChan {
			h.handleError(err)
		}
	})
	h.Driver.ReceiveMessage(ctx, h.Queue, errChan, h.handleMsg)
	close(errChan) // 关闭错误通道, 退出错误处理协程
}

// handleMsg 处理消息
// 根据处理器配置对消息处理进行封装
// 屏蔽复杂度, 确保消息高效无误的流转
// 若返回值为true则表示处理成功, 将删除该消息
// 若返回值为false则表示处理失败, 消息将延迟重试
func (h *Handler) handleMsg(data []byte) bool {
	var per error
	defer h.handleError(per)
	defer utils.PanicToError(&per)
	var msg Message
	decode(data, &msg)
	allow, err := h.Idempotent.Acquire(msg.BizUID)
	if err != nil {
		h.handleError(err)
		allow, err = false, nil
	}
	if allow == false {
		// allow为false不允许的情况下进行二次确认
		if allow = h.ensure(&msg); !allow {
			// 二次确认消息已处理代表消息可删除
			return true
		}
	}
	if h.HandleFunc(&msg) == false {
		// 处理失败累加次数
		msg.Retried += 1
		// 计算多少秒后进行重试
		delay := h.RetryDelay(msg.Retried)
		if delay < 0 {
			// 不进行重试, 死信存储
			err = h.DLStorage.Store(h.Queue, data)
		} else {
			// 重新发布, 进入延迟重试
			err = h.Driver.SendToQueue(h.Queue, encode(msg), delay)
		}
		h.handleError(err)
		// 处理失败, 释放控制权
		h.handleError(h.Idempotent.Release(msg.BizUID))
	}
	return err == nil
}

// ensure 幂等性二次确认
func (h *Handler) ensure(msg *Message) bool {
	if h.EnsureFunc == nil {
		return false
	}
	return h.EnsureFunc(msg)
}

// handleError 异常错误处理
func (h *Handler) handleError(err error) {
	if err == nil {
		return
	}
	if h.ErrorFunc == nil {
		log.Printf("easy-bus: handler %q has an error, %v", h.Queue, err)
	} else {
		h.ErrorFunc(err)
	}
}
