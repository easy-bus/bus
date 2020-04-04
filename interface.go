package bus

import (
	"context"
	"time"
)

type LoggerInterface interface {
	Errorf(format string, args ...interface{})
}

// IdempotentInterface 幂等性接口
type IdempotentInterface interface {
	// Acquire 获取key的操作权
	// 若返回值为true表示获取成功, 允许操作
	// 若返回值为false表示获取失败, 不允许操作
	Acquire(key string) (bool, error)

	// Release 释放key的操作权
	Release(key string) error
}

// DLStorageInterface 死信存储接口
type DLStorageInterface interface {
	// Store 存储队列中无法处理的消息内容
	Store(queue string, data []byte) error
}

// TXStorageInterface 预发存储接口
type TXStorageInterface interface {
	// Store 将消息预存
	// id 返回存储后的唯一标识
	Store(data []byte) (id string, err error)

	// Fetch 根据标识取出消息
	Fetch(id string) (data []byte, err error)

	// Remove 根据标识移除消息
	Remove(id string) error
}

// DriverInterface 驱动接口
type DriverInterface interface {
	// CreateQueue 创建队列
	// name 队列名称, 确保唯一
	// delay 队列消息延迟时长, 指定时长后方可被消费者获取
	CreateQueue(name string, delay time.Duration) error

	// CreateTopic 创建主题
	// name 主题名称, 确保唯一
	CreateTopic(name string) error

	// Subscribe 订阅主题
	// topic 订阅的主题名称
	// queue 消息流转队列名称
	// routeKey 路由键, 匹配的消息才会被路由到队列
	// 请格外注意, 关于routeKey的使用具体实现会有不同
	Subscribe(topic, queue, routeKey string) error

	// UnSubscribe 取消订阅, 参数同Subscribe
	UnSubscribe(topic, queue, routeKey string) error

	// SendToQueue 发送消息至队列
	// queue 发送目标队列名称
	// content 发送消息字节内容
	// delay 消息延迟时长, 优先级高于CreateQueue时指定的延迟
	SendToQueue(queue string, content []byte, delay time.Duration) error

	// SendToTopic 发送消息至主题
	// topic 发送目标主题名称
	// content 发送消息字节内容
	// routeKey 路由键, 仅路由到匹配的订阅队列
	SendToTopic(topic string, content []byte, routeKey string) error

	// ReceiveMessage 监听队列获取消息
	// ctx 上下文, 用于中断监听
	// queue 接受消息的队列名称
	// errChan 异常错误传输通道
	// handler 消息回调处理函数
	ReceiveMessage(ctx context.Context, queue string, errChan chan error, handler func([]byte) bool)
}
