package bus

import (
	"fmt"
)

// throw 抛出异常错误
func throw(format string, args ...interface{}) {
	panic(fmt.Sprintf("easy-bus: %s", fmt.Sprintf(format, args...)))
}

// errorWrap 包装错误信息,更易调试
func errorWrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s, err = %v", msg, err)
}
