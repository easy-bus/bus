package bus

import (
	"context"
	"fmt"
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
		throw("sender [%s] the timeout of tx option must greater than zero", topic)
	}
	if to.EnsureFunc == nil {
		throw("sender [%s] the ensure func of tx option is missing", topic)
	}
	if to.TxStorage == nil {
		throw("sender [%s] the storage of tx option is missing", topic)
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

	// Logger 异常日志
	Logger LoggerInterface

	// TxOptions 事务配置
	TxOptions *TxOptions

	// ready 是否就绪
	ready bool

	txHandler *Handler
}

// Prepare 创建主题和日志队列
func (s *Sender) Prepare() *Sender {
	if s.ready {
		return s
	}
	if s.Driver == nil {
		throw("sender [%s] missing driver instance", s.Topic)
	}
	if s.Logger == nil {
		s.Logger = stderrLogger{}
	}
	if err := s.Driver.CreateTopic(s.Topic); err != nil {
		throw("sender [%s] create topic error, %v", s.Topic, err)
	}
	if s.TxOptions != nil {
		s.TxOptions.prepare(s.Topic)
		s.txHandler = &Handler{
			Queue:  s.TxOptions.recordQueue,
			Driver: s.Driver,
			Logger: s.Logger,
			HandleFunc: func(log *Message) bool {
				var id string
				log.Scan(&id)
				data, err := s.TxOptions.TxStorage.Fetch(id)
				if err != nil {
					s.Logger.Errorf("sender [%s] tx fetch failed, %v", s.Topic, err)
					return false
				} else if data == nil {
					// 已经发布成功
					s.txRemove(id)
					return true
				}
				var msg Message
				decode(data, &msg)
				if s.TxOptions.EnsureFunc(&msg) {
					// 事务处理成功, 消息未发送
					err = s.Driver.SendToTopic(s.Topic, data, msg.RouteKey)
					if err == nil {
						s.txRemove(id)
						return true
					}
					s.Logger.Errorf("sender [%s] with route key [%s] failed, %v", s.Topic, msg.RouteKey, err)
					return false
				} else {
					// 事务未处理成功, 消息丢弃
					s.txRemove(id)
					return true
				}
			},
			RetryDelay: s.TxOptions.RetryDelay,
			EnsureFunc: func(msg *Message) (allow bool) { return true },
		}
		s.txHandler.Prepare()
		go s.txHandler.RunCtx(s.TxOptions.Context)
	}
	s.ready = true
	return s
}

// Send 发送消息
// msg 发送的消息结构体
// localTx 本地事务执行函数
func (s *Sender) Send(msg *Message, localTx ...func() error) (err error) {
	if s.ready == false {
		throw("sender [%s] has not prepared", s.Topic)
	}
	defer utils.HandlePanic(func(i interface{}) {
		err = fmt.Errorf("sender [%s] panic, %v", s.Topic, i)
	})
	if len(localTx) == 0 || localTx[0] == nil {
		// 未使用事务, 直接发布至主题
		if err := s.Driver.SendToTopic(s.Topic, encode(msg), msg.RouteKey); err != nil {
			return fmt.Errorf("sender [%s] with route key [%s] failed, %v", s.Topic, msg.RouteKey, err)
		}
	} else if s.TxOptions == nil {
		return fmt.Errorf("sender [%s] missing tx options", s.Topic)
	} else {
		data := encode(msg)
		// 消息预发存储
		id, err := s.TxOptions.TxStorage.Store(data)
		if err != nil {
			return fmt.Errorf("sender [%s] tx store failed, %v", s.Topic, err)
		}
		// 将操作日志发送至队列
		err = s.Driver.SendToQueue(
			s.TxOptions.recordQueue,
			encode(MessageWithId(id, id, "")),
			s.TxOptions.Timeout,
		)
		if err != nil {
			return fmt.Errorf(
				"sender [%s] send to queue [%s] with delay [%d] failed, %v",
				s.Topic, s.TxOptions.recordQueue, s.TxOptions.Timeout, err,
			)
		}
		// 执行本地事务
		if err := localTx[0](); err != nil {
			s.txRemove(id) // 事务失败即可清理
			return err
		}
		// 此时无需关心消息是否发送成功, 可依靠日志补偿处理
		if err := s.Driver.SendToTopic(s.Topic, data, msg.RouteKey); err != nil {
			s.Logger.Errorf("sender [%s] with route key [%s] failed, %v", s.Topic, msg.RouteKey, err)
		} else {
			s.txRemove(id) // 发送成功即可清理
		}
	}
	return nil
}

// Wait 等待退出
func (s *Sender) Wait() {
	if s.txHandler == nil {
		return
	}
	s.txHandler.Wait()
}

// txRemove 内部封装,便于使用
func (s *Sender) txRemove(id string) {
	if err := s.TxOptions.TxStorage.Remove(id); err != nil {
		s.Logger.Errorf("sender [%s] tx remove failed, %v", s.Topic, err)
	}
}
