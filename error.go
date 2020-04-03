package bus

import (
	"fmt"
)

// errorWrap 包装错误信息,更易调试
func errorWrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s, err = %v", msg, err)
}
