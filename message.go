package bus

import (
	"encoding/json"
)

// Message 消息结构体
type Message struct {
	// BizUID 消息唯一标识
	// 无特殊业务含义, 通常用于幂等性处理防止重复消费
	BizUID string `json:"b,omitempty"`

	// Payload 原始消息内容
	Payload []byte `json:"p,omitempty"`

	// Retried 记录消息重试次数
	Retried int `json:"r,omitempty"`

	// RouteKey 路由键
	RouteKey string `json:"k,omitempty"`
}

// Scan 将消息内容赋值给目标参数
func (m *Message) Scan(dest interface{}) { decode(m.Payload, dest) }

// MessageAutoId 实例化消息
func MessageAutoId(payload interface{}, routeKey string) *Message {
	return MessageWithId(generateSeqId(), payload, routeKey)
}

// MessageWithId 实例化消息
func MessageWithId(id string, payload interface{}, routeKey string) *Message {
	return &Message{
		BizUID:   id,
		Payload:  encode(payload),
		RouteKey: routeKey,
	}
}

// encode 数据编码
func encode(data interface{}) []byte {
	bts, err := json.Marshal(data)
	if err != nil {
		throw("easy-bus: encode error, %v", err)
	}
	return bts
}

// decode 数据解码
func decode(bts []byte, dest interface{}) {
	err := json.Unmarshal(bts, dest)
	if err != nil {
		throw("easy-bus: decode [%s] error, %v", string(bts), err)
	}
}
