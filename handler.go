package bus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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
	sync.Once
	Context context.Context

	// Queue 处理队列的名称
	Queue string

	// Delay 消息处理延迟时长
	Delay time.Duration

	// Subscribe 订阅配置
	Subscribe Subscribe

	// Driver 驱动实例
	Driver DriverInterface

	// Logger 异常日志
	Logger LoggerInterface

	// DLStorage 死信存储
	// 无法处理的消息最终流转到这里
	DLStorage DLStorageInterface

	// Idempotent 幂等判断实现
	// 防止消息被重复处理保证数据一致性
	// 若幂等性判断自身异常则可能导致判断失效
	// 因此再严格一致的场景下配置EnsureFn进行二次确认
	Idempotent IdempotentInterface

	// HandleFunc 消息处理回调函数
	// 若返回值为true则表示处理成功, 将删除该消息
	// 若返回值为false则表示处理失败, 消息将延迟重试
	HandleFunc func(msg *Message) (done bool)

	// EnsureFunc 幂等性的二次确认
	// 请一定要注意布尔返回值的代表含义
	// 若返回值为true表示未处理, 即允许处理
	// 若返回值为false表示已处理, 即不允许处理
	// 若使用场景不严格要求数据一致的可以不用配置
	EnsureFunc func(msg *Message) (allow bool)

	// RetryDelay 重试延迟机制
	// 返回值为重试间隔, 若 < 0 则代表不进行重试
	RetryDelay func(attempts int) time.Duration

	// ready 是否就绪
	ready bool

	// 退出信号
	quit chan struct{}

	// 是否运行
	running int32
}

// Prepare 准备就绪
func (h *Handler) Prepare() *Handler {
	h.Do(func() {
		if h.Queue == "" {
			throw("the handler missing queue name")
		}
		if h.Driver == nil {
			throw("the handler [%s] missing driver instance", h.Queue)
		}
		if h.HandleFunc == nil {
			throw("the handler [%s] missing handle function", h.Queue)
		}
		if h.Logger == nil {
			h.Logger = stderrLogger{}
		}
		if h.DLStorage == nil {
			h.DLStorage = nullDLStorage{}
		}
		if h.Idempotent == nil {
			h.Idempotent = nullIdempotent{}
		}
		if h.EnsureFunc == nil {
			h.EnsureFunc = func(*Message) bool { return false }
		}
		if h.RetryDelay == nil {
			h.RetryDelay = func(int) time.Duration { return -1 }
		}
		h.initDriver()
		h.ready = true
		h.quit = make(chan struct{})
	})
	return h
}

// Run 启动处理器
func (h *Handler) Run() {
	if h.ready == false {
		throw("run is forbidden when the handler [%s] has not prepared", h.Queue)
	}
	if !atomic.CompareAndSwapInt32(&h.running, 0, 1) {
		return // 已在运行中
	}
	errChan := make(chan error)
	goroutine(func() {
		for err := range errChan {
			h.initDriver() // 队列级错误尝试恢复
			h.Logger.Errorf("handler [%s] error, %v", h.Queue, err)
		}
	})
	ticker := time.NewTicker(time.Minute)
	goroutine(func() {
		for range ticker.C {
			h.handleRetry()
		}
	})
	h.Driver.ReceiveMessage(h.Context, h.Queue, errChan, h.handleMsg)
	close(errChan) // 关闭错误通道, 退出错误处理协程
	ticker.Stop()  // 关闭重试定时器, 退出重试处理协程
	atomic.StoreInt32(&h.running, 0)
	h.quit <- struct{}{}
}

// RunCtx 启动处理器
func (h *Handler) RunCtx(ctx context.Context) {
	h.Context = ctx
	h.Run()
}

// Wait 等待退出
func (h *Handler) Wait() { <-h.quit }

// handleMsg 处理消息
// 根据处理器配置对消息处理进行封装
// 屏蔽复杂度, 确保消息高效无误的流转
// 若返回值为true则表示处理成功, 将删除该消息
// 若返回值为false则表示处理失败, 消息将延迟重试
func (h *Handler) handleMsg(data []byte) (done bool) {
	defer handlePanic(func(i interface{}) {
		done = h.DLStorage.Store(h.Queue, data) == nil
		str := "handler [%s] panic: %v, data: %s, call stack: \n%s"
		h.Logger.Errorf(str, h.Queue, i, string(data), stackTrace(0))
	})
	var msg Message
	decode(data, &msg)
	key := h.Queue + "." + msg.BizUID
	allow, err := h.Idempotent.Acquire(key)
	if err != nil {
		allow = false // 置为false进行二次确认
		h.Logger.Errorf("handler [%s] idempotent acquired failed, %v", err)
	}
	if !allow && !h.EnsureFunc(&msg) {
		return true // 二次确认
	} else if h.HandleFunc(&msg) {
		return true // 处理成功
	}
	// 处理失败, 释放控制权
	if err := h.Idempotent.Release(key); err != nil {
		h.Logger.Errorf("handler [%s] idempotent release failed, %v", err)
	}
	// 处理失败累加次数
	msg.Retried += 1
	// 计算多少秒后进行重试
	if delay := h.RetryDelay(msg.Retried); delay < 0 {
		if err := h.DLStorage.Store(h.Queue, data); err != nil {
			h.Logger.Errorf("handler [%s] dl store failed, v", h.Queue, err)
			return false // 死信储存失败
		}
	} else {
		// 重新发布, 进入延迟重试
		if err := h.Driver.SendToQueue(h.Queue, encode(msg), delay); err != nil {
			h.Logger.Errorf("handler [%s] send to queue with delay [%d] failed, %v", h.Queue, delay, err)
			return false // 重试发送失败
		}
	}
	return true
}

// handleRetry 重试处理失败消息
func (h *Handler) handleRetry() {
	rows, err := h.DLStorage.Fetch(h.Queue)
	if err != nil {
		h.Logger.Errorf("retry fetch [%s] error, %v", h.Queue, err)
		return
	}
	for id, data := range rows {
		if h.handleMsg(data) {
			err = h.DLStorage.Remove(id)
			if err != nil {
				h.Logger.Errorf("retry delete [%s] error, %v", id, err)
			}
		}
	}
}

// initDriver 驱动初始化
func (h *Handler) initDriver() {
	if err := h.Driver.CreateQueue(h.Queue, h.Delay); err != nil {
		throw("then handler [%s] create queue failed, %v", h.Queue, err)
	}
	if h.Subscribe.Topic != "" {
		if err := h.Driver.Subscribe(h.Subscribe.Topic, h.Queue, h.Subscribe.RouteKey); err != nil {
			throw("then handler [%s] subscribe topic [%s] failed, %v", h.Queue, h.Subscribe.Topic, err)
		}
	}
}
