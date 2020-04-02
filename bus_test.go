package bus

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type User struct {
	Id   string
	Name string
	Info map[string]string
}

func TestMessage(t *testing.T) {
	u1 := User{
		Id:   "u1",
		Name: "Jim",
		Info: map[string]string{
			"age": "1",
			"sex": "Male",
		},
	}
	m1 := MessageAutoId(u1, "")
	u2 := User{}
	m2 := new(Message)
	decode(encode(m1), m2)
	assert.Equal(t, m1, m2)
	m2.Scan(&u2)
	assert.Equal(t, u1, u2)
}

func TestSender_handleError(t *testing.T) {
	var num int
	s := Sender{
		Topic: "test.handle_error",
		ErrorFunc: func(err error) {
			num += 1
		},
	}
	s.handleError(nil)
	s.handleError(errors.New("error"))
	s.ErrorFunc = nil
	s.handleError(errors.New("log println error"))
	assert.Equal(t, num, 1)
}

func TestHandler_handleError(t *testing.T) {
	var num int
	h := Handler{
		Queue: "test.handle_error",
		ErrorFunc: func(err error) {
			num += 1
		},
	}
	h.handleError(nil)
	h.handleError(errors.New("error"))
	h.ErrorFunc = nil
	h.handleError(errors.New("log println error"))
	assert.Equal(t, num, 1)
}

var itDLS *internalDLStorage
var itTXS *internalTXStorage
var driver *mockDriver
var sender Sender
var handler Handler

func prepare() {
	itDLS = &internalDLStorage{}
	itTXS = &internalTXStorage{}
	driver = &mockDriver{
		itd: &internalDriver{},
	}
	sender = Sender{
		Topic:  "sender.basic",
		Driver: driver,
	}
	handler = Handler{
		Queue:  "handler.basic",
		Driver: driver,
		Subscribe: Subscribe{
			Topic: sender.Topic,
		},
		HandleFunc: func(msg *Message) bool {
			fmt.Println(msg)
			return true
		},
	}
}

func mockAllNormal() {
	driver.On("CreateQueue", mock.Anything, mock.Anything).Return(nil)
	driver.On("CreateTopic", mock.Anything).Return(nil)
	driver.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	driver.On("UnSubscribe", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	driver.On("SendToQueue", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	driver.On("SendToTopic", mock.Anything, mock.Anything, mock.Anything).Return(nil)
}

func mockSendToTopicError() {
	driver.On("CreateQueue", mock.Anything, mock.Anything).Return(nil)
	driver.On("CreateTopic", mock.Anything).Return(nil)
	driver.On("Subscribe", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	driver.On("UnSubscribe", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	driver.On("SendToQueue", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	driver.On("SendToTopic", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error"))
}

func TestIdempotent(t *testing.T) {
	prepare()
	mockAllNormal()
	var num1, num2 uint32
	exitChan := make(chan struct{})
	originMsg := MessageAutoId("message.idempotent", "")
	handler.Idempotent = &internalIdempotent{}
	handler.HandleFunc = func(msg *Message) bool {
		atomic.AddUint32(&num1, 1)
		assert.EqualValues(t, originMsg, msg)
		exitChan <- struct{}{}
		return true
	}
	handler.EnsureFunc = func(msg *Message) bool {
		atomic.AddUint32(&num2, 1)
		exitChan <- struct{}{}
		return false
	}
	sender.Prepare()
	var ctx, cancelFunc = context.WithCancel(context.TODO())
	go handler.Prepare().RunCtx(ctx)
	for i := 0; i < 5; i++ {
		sender.Send(originMsg)
		<-exitChan
	}
	cancelFunc()
	assert.EqualValues(t, uint32(1), num1)
	assert.EqualValues(t, uint32(4), num2)
}

func TestDLStorage(t *testing.T) {
	prepare()
	mockAllNormal()
	exitChan := make(chan struct{})
	originMsg := MessageAutoId("message.dl-storage", "")
	handler.DLStorage = itDLS
	handler.HandleFunc = func(msg *Message) bool {
		assert.EqualValues(t, originMsg, msg)
		return false
	}
	handler.EnsureFunc = func(msg *Message) bool {
		return true
	}
	handler.RetryDelay = func(attempts int) time.Duration {
		exitChan <- struct{}{}
		return -1
	}
	sender.Prepare()
	var ctx, cancelFunc = context.WithCancel(context.TODO())
	go handler.Prepare().RunCtx(ctx)
	sender.Send(originMsg)

	<-exitChan
	cancelFunc()
	time.Sleep(time.Millisecond)

	assert.Equal(t, encode(originMsg), itDLS.dataMap[handler.Queue][0])
}

func TestTransaction(t *testing.T) {
	prepare()
	mockSendToTopicError()
	var num1, num2 uint32
	exitChan := make(chan struct{})
	originMsg := MessageAutoId("message.transaction", "")
	sender.TxOptions = &TxOptions{
		Timeout: time.Millisecond,
		EnsureFunc: func(msg *Message) bool {
			assert.EqualValues(t, originMsg, msg)
			if atomic.AddUint32(&num2, 1) > 2 {
				close(exitChan)
				return false
			}
			return true
		},
		RetryDelay: func(attempts int) time.Duration {
			return 0
		},
		TxStorage: itTXS,
	}
	handler.HandleFunc = func(msg *Message) bool {
		atomic.AddUint32(&num1, 1)
		return true
	}
	handler.EnsureFunc = func(msg *Message) bool {
		return true
	}
	sender.Prepare()
	var ctx, cancelFunc = context.WithCancel(context.TODO())
	go handler.Prepare().RunCtx(ctx)
	sender.Send(originMsg, func() bool {
		return false
	})
	sender.Send(originMsg, func() bool {
		return true
	})

	<-exitChan
	cancelFunc()
	time.Sleep(time.Millisecond)

	assert.Equal(t, uint32(0), num1)
	assert.Equal(t, uint32(3), num2)
}
