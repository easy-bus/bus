package bus

import (
	"github.com/sony/sonyflake"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"time"
)

var sf *sonyflake.Sonyflake

func init() {
	sf = sonyflake.NewSonyflake(
		sonyflake.Settings{
			StartTime: time.Unix(1567267200, 0),
		},
	)
	rand.Seed(time.Now().UnixNano())
}

// goroutine 协程执行
func goroutine(fn func()) { go fn() }

// handlePanic 使用回调函数处理panic
func handlePanic(hs ...func(interface{})) {
	if r := recover(); r != nil {
		for _, handler := range hs {
			handler(r)
		}
	}
}

// stackTrace 获取堆栈踪迹
func stackTrace(size int) []byte {
	if size <= 0 {
		size = math.MaxUint16
	}
	stacktrace := make([]byte, size)
	return stacktrace[:runtime.Stack(stacktrace, false)]
}

// generateSeqId 生成自增ID
func generateSeqId() string {
	id, _ := sf.NextID()
	return strconv.FormatUint(id, 36)
}
