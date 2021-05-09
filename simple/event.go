package simple

import "github.com/easy-bus/bus"

type Extend map[string]interface{}

func (ex Extend) Int(key string) int {
	return int(ex.Float64(key))
}

func (ex Extend) Int32(key string) int32 {
	return int32(ex.Float64(key))
}

func (ex Extend) Int64(key string) int64 {
	return int64(ex.Float64(key))
}

func (ex Extend) Extend(key string) Extend {
	if v, ok := ex[key]; ok {
		return v.(map[string]interface{})
	}
	return Extend{}
}

func (ex Extend) String(key string) string {
	if v, ok := ex[key]; ok {
		return v.(string)
	}
	return ""
}

func (ex Extend) Float64(key string) float64 {
	if v, ok := ex[key]; ok {
		return v.(float64)
	}
	return 0
}

func (ex Extend) Strings(key string) []string {
	if v, ok := ex[key]; ok {
		iv := v.([]interface{})
		sv := make([]string, len(iv))
		for i, v := range iv {
			sv[i] = v.(string)
		}
		return sv
	}
	return []string{}
}

// Common
type Common struct {
	OP string `json:"op"` // 操作FLAG
	ID string `json:"id"` // 数据主键
}

func LoadCommon(msg *bus.Message) *Common {
	common := new(Common)
	msg.Scan(common)
	return common
}

// CommonEX
type CommonEX struct {
	OP string `json:"op"` // 操作FLAG
	ID string `json:"id"` // 数据主键
	EX Extend `json:"ex"` // 扩展数据
}

func LoadCommonEx(msg *bus.Message) *CommonEX {
	commonEx := new(CommonEX)
	msg.Scan(commonEx)
	return commonEx
}

// BatchEX
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
