package simple

import (
	"context"
	"github.com/easy-bus/bus"
)

var driver bus.DriverInterface
var dlStorage bus.DLStorageInterface
var txStorage bus.TXStorageInterface
var idempotent bus.IdempotentInterface
var logger bus.LoggerInterface

var senderGroup = make(senders, 0)
var handlerGroup = make(handlers, 0)
var cancelGroup = make(cancels, 0)

func StartUp(
	drv bus.DriverInterface,
	dls bus.DLStorageInterface,
	txs bus.TXStorageInterface,
	ide bus.IdempotentInterface,
	log bus.LoggerInterface,
) {
	driver, dlStorage, txStorage, idempotent, logger = drv, dls, txs, ide, log
	for _, sender := range senderGroup {
		sender.Prepare()
	}
	for _, handler := range handlerGroup {
		go handler.Prepare().Run()
	}
}

func ShutDown() {
	cancelGroup.exec()  // 发送cancel取消
	senderGroup.wait()  // 等待sender结束
	handlerGroup.wait() // 等待handler结束
}

// senders 发送器集合
type senders []*bus.Sender

func (sds *senders) add(sender *bus.Sender) *bus.Sender {
	*sds = append(*sds, sender)
	return sender
}

func (sds senders) wait() {
	for i := range sds {
		sds[i].Wait()
	}
}

// handlers 处理器集合
type handlers []*bus.Handler

func (hds *handlers) add(handler *bus.Handler) *bus.Handler {
	*hds = append(*hds, handler)
	return handler
}

func (hds handlers) wait() {
	for i := range hds {
		hds[i].Wait()
	}
}

// cancels 退出Bus相关协程的函数集合
type cancels []context.CancelFunc

func (ccs *cancels) exec() {
	for _, cancel := range *ccs {
		cancel()
	}
}

func (ccs *cancels) newCtx() context.Context {
	ctx, cancel := context.WithCancel(context.TODO())
	*ccs = append(*ccs, cancel)
	return ctx
}
