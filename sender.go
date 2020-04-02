package bus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/letsfire/utils"
)

// TxOptions 事务配置
type TxOptions struct {
	Context context.Context
	// Timeout 事务处理时长
	// 启用事务的消息不会立即发布给消费者
	// 当本地事务回调执行返回true才会正式发布
	// 详见事务流程图 ./tx_flow.png
	Timeout time.Duration

	// EnsureFunc 事务完成确认
	// 请一定要注意布尔返回值的代表含义
	// 若返回值为true则表示事务已处理, 发布消息
	// 若返回值为false则表示事务未处理, 撤销消息
	EnsureFunc func(msg *Message) (done bool)

	// RetryDelay 重试延迟机制
	// 返回值为重试间隔, 若 < 0 则代表不进行重试
	RetryDelay func(attempts int) time.Duration

	// TxStorage 事务消息存储
	TxStorage TXStorageInterface

	// recordQueue 日志队列
	recordQueue string
}

func (to *TxOptions) prepare(topic string) {
	if to.Timeout <= 0 {
		panic("easy-bus: the timeout of tx option must greater than zero")
	}
	if to.EnsureFunc == nil {
		panic("easy-bus: the ensure func of tx option is missing")
	}
	if to.TxStorage == nil {
		panic("easy-bus: the storage of tx option is missing")
	}
	if to.Context == nil {
		to.Context = context.Background()
	}
	if to.RetryDelay == nil {
		to.RetryDelay = func(attempts int) time.Duration {
			if attempts > 5 {
				return time.Minute
			}
			return time.Duration(attempts) * 10 * time.Second
		}
	}
	to.recordQueue = fmt.Sprintf("%s.tx-record", topic)
}

// Sender 发送器
type Sender struct {
	// Topic 发送主题
	Topic string

	// Driver 驱动实例
	Driver DriverInterface

	// ErrorFunc 异常错误处理
	// 通常用于记录日志并上报通知
	ErrorFunc func(err error)

	// TxOptions 事务配置
	TxOptions *TxOptions

	// ready 是否就绪
	ready bool
}

func (s *Sender) Reset() {
	if !s.ready {
		return
	}
	if s.TxOptions != nil {
		s.TxOptions.Context.Done()
	}
	s.ready = false
}

// Prepare 创建主题和日志队列
func (s *Sender) Prepare() *Sender {
	if s.ready {
		return s
	}
	if s.Driver == nil {
		panic("easy-bus: the sender missing driver instance")
	}
	if err := s.Driver.CreateTopic(s.Topic); err != nil {
		panic(fmt.Sprintf("easy-bus: the sender create topic error, %v", err))
	}
	if s.TxOptions != nil {
		s.TxOptions.prepare(s.Topic)
		handler := Handler{
			Queue:     s.TxOptions.recordQueue,
			Driver:    s.Driver,
			ErrorFunc: s.ErrorFunc,
			HandleFunc: func(log *Message) bool {
				var id string
				log.Scan(&id)
				data, err := s.TxOptions.TxStorage.Fetch(id)
				if err != nil {
					s.handleError(err)
					return false
				} else if data == nil {
					// 已经发布成功
					s.handleError(s.TxOptions.TxStorage.Remove(id))
					return true
				}
				var msg Message
				decode(data, &msg)
				if s.TxOptions.EnsureFunc(&msg) {
					// 事务处理成功, 消息未发送
					err = s.Driver.SendToTopic(s.Topic, data, msg.RouteKey)
					if err == nil {
						s.handleError(s.TxOptions.TxStorage.Remove(id))
						return true
					}
					return false
				} else {
					// 事务未处理成功, 消息丢弃
					s.handleError(s.TxOptions.TxStorage.Remove(id))
					return true
				}
			},
			EnsureFunc: func(msg *Message) (allow bool) {
				return true
			},
			RetryDelay: s.TxOptions.RetryDelay,
		}
		go handler.Prepare().RunCtx(s.TxOptions.Context)
	}
	s.ready = true
	return s
}

// Send 发送消息
// msg 发送的消息结构体
// localTx 本地事务执行函数
// 若返回值为true表示本地事务执行成功, 则提交消息
// 若返回值为false表示本地事务执行失败, 则回滚消息
func (s *Sender) Send(msg *Message, localTx ...func() bool) bool {
	if s.ready == false {
		panic(fmt.Sprintf("easy-bus: send is forbidden when the sender %q has not prepared", s.Topic))
	}
	var err error
	defer s.handleError(err)
	defer utils.PanicToError(&err)
	if len(localTx) == 0 {
		// 未使用事务, 直接发布至主题
		err = s.Driver.SendToTopic(s.Topic, encode(msg), msg.RouteKey)
		return err == nil
	} else if s.TxOptions == nil {
		err = errors.New("easy-bus: local tx is forbidden when sender missing tx options")
		return false
	} else {
		data := encode(msg)
		// 消息预发存储
		id, err := s.TxOptions.TxStorage.Store(data)
		if err != nil {
			return false
		}
		// 将操作日志发送至队列
		err = s.Driver.SendToQueue(s.TxOptions.recordQueue, encode(NewMessage(id, "")), 0)
		if err != nil {
			return false
		}
		// 执行本地事务
		if localTx[0]() {
			// 此时无需关心消息是否发送成功, 可依靠日志补偿处理
			err = s.Driver.SendToTopic(s.Topic, data, msg.RouteKey)
			if err == nil {
				err = s.TxOptions.TxStorage.Remove(id)
			}
			return true
		}
		err = s.TxOptions.TxStorage.Remove(id)
		return false
	}
}

// handleError 异常错误处理
func (s *Sender) handleError(err error) {
	if err == nil {
		return
	}
	if s.ErrorFunc == nil {
		log.Printf("easy-bus: sender %q has an error, %v", s.Topic, err)
	} else {
		s.ErrorFunc(err)
	}
}
