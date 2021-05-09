package bus

import (
	"fmt"
)

// throw 抛出异常错误
func throw(format string, args ...interface{}) {
	panic(fmt.Sprintf("easy-bus: %s", fmt.Sprintf(format, args...)))
}
