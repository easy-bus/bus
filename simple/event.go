package simple

import (
	"fmt"
	"github.com/easy-bus/bus"
)

// Common 通用基础消息
type Common struct {
	OP string `json:"op"` // 操作FLAG
	ID string `json:"id"` // 数据主键
}

func (c *Common) Message(unique bool) *bus.Message {
	if unique == false {
		return bus.MessageAutoId(c, c.OP)
	}
	id := fmt.Sprintf("%s.%s", c.OP, c.ID)
	return bus.MessageWithId(id, c, c.OP)
}

func LoadCommon(msg *bus.Message) *Common {
	common := new(Common)
	msg.Scan(common)
	return common
}

// CommonEX 通用扩展消息
type CommonEX struct {
	OP string `json:"op"` // 操作FLAG
	ID string `json:"id"` // 数据主键
	EX Extend `json:"ex"` // 扩展数据
}

func (c *CommonEX) Message(unique bool) *bus.Message {
	if unique == false {
		return bus.MessageAutoId(c, c.OP)
	}
	id := fmt.Sprintf("%s.%s", c.OP, c.ID)
	return bus.MessageWithId(id, c, c.OP)
}

func LoadCommonEx(msg *bus.Message) *CommonEX {
	commonEx := new(CommonEX)
	msg.Scan(commonEx)
	return commonEx
}

// BatchEX 批量扩展消息
type BatchEX struct {
	OP  string   `json:"op"`  // 操作FLAG
	IDS []string `json:"ids"` // 数据主键
	EX  Extend   `json:"ex"`  // 扩展数据
}

func LoadBatchEx(msg *bus.Message) *BatchEX {
	batchEx := new(BatchEX)
	msg.Scan(batchEx)
	return batchEx
}
