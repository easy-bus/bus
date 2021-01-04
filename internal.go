package bus

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/letsfire/utils"
)

// stderrLogger 默认错误日志
type stderrLogger struct{}

func (stderrLogger) Errorf(format string, args ...interface{}) {
	log.Println(fmt.Sprintf("easy-bus: %s", fmt.Sprintf(format, args...)))
}

// nullIdempotent 空的幂等实现
type nullIdempotent struct{}

func (ni nullIdempotent) Acquire(key string) (bool, error) { return false, nil }

func (ni nullIdempotent) Release(key string) error { return nil }

// internalIdempotent 内部幂等实现
type internalIdempotent struct {
	sync.Mutex
	dataMap map[string]bool
}

func (ii *internalIdempotent) Acquire(key string) (bool, error) {
	ii.Lock()
	defer ii.Unlock()
	if ii.dataMap == nil {
		ii.dataMap = make(map[string]bool)
	}
	if _, ok := ii.dataMap[key]; ok {
		return false, nil
	}
	ii.dataMap[key] = true
	return true, nil
}

func (ii *internalIdempotent) Release(key string) error {
	delete(ii.dataMap, key)
	return nil
}

// nullDLStorage 空的死信存储
type nullDLStorage struct{}

func (nd nullDLStorage) Store(queue string, data []byte) error { return nil }

func (nd nullDLStorage) Fetch(queue string) (map[string][]byte, error) {
	return nil, nil
}

func (nd nullDLStorage) Remove(id string) error { return nil }

// internalDLStorage 内部死信存储
type internalDLStorage struct {
	index   map[string]string
	dataMap map[string]map[string][]byte
}

func (id *internalDLStorage) Store(queue string, data []byte) error {
	if id.dataMap == nil {
		id.index = make(map[string]string)
		id.dataMap = make(map[string]map[string][]byte)
	}
	if _, ok := id.dataMap[queue]; !ok {
		id.dataMap[queue] = make(map[string][]byte)
	}
	pid := strconv.Itoa(len(id.dataMap[queue]))
	id.index[pid], id.dataMap[queue][pid] = queue, data
	return nil
}

func (id *internalDLStorage) Fetch(queue string) (map[string][]byte, error) {
	return id.dataMap[queue], nil
}

func (id *internalDLStorage) Remove(pid string) error {
	queue := id.index[pid]
	delete(id.dataMap[queue], pid)
	return nil
}

// internalTXStorage 内部事务存储
type internalTXStorage struct {
	dataMap map[string][]byte
}

func (it *internalTXStorage) Store(data []byte) (string, error) {
	if it.dataMap == nil {
		it.dataMap = make(map[string][]byte)
	}
	id := utils.GenerateSeqId()
	it.dataMap[id] = data
	return id, nil
}

func (it *internalTXStorage) Fetch(id string) ([]byte, error) {
	return it.dataMap[id], nil
}

func (it *internalTXStorage) Remove(id string) error {
	delete(it.dataMap, id)
	return nil
}

// internalDriver 内部驱动实现
type internalDriver struct {
	queues   map[string]*internalQueue
	relation map[string]map[string]map[string]*internalQueue
}

// internalQueue 内部队列结构
type internalQueue struct {
	name    string
	delay   time.Duration
	msgChan chan internalData
}

// internalData 内部消息结构
type internalData struct {
	data  []byte
	delay time.Duration
}

func (id *internalDriver) CreateQueue(name string, delay time.Duration) error {
	if id.queues == nil {
		id.queues = make(map[string]*internalQueue)
	}
	id.queues[name] = &internalQueue{
		name:    name,
		delay:   delay,
		msgChan: make(chan internalData, 9),
	}
	return nil
}

func (id *internalDriver) CreateTopic(name string) error {
	if id.relation == nil {
		id.relation = make(map[string]map[string]map[string]*internalQueue)
	}
	if _, ok := id.relation[name]; !ok {
		id.relation[name] = make(map[string]map[string]*internalQueue)
	}
	return nil
}

func (id *internalDriver) Subscribe(topic, queue, routeKey string) error {
	if _, ok := id.relation[topic][queue]; !ok {
		id.relation[topic][queue] = make(map[string]*internalQueue)
	}
	id.relation[topic][queue][routeKey] = id.queues[queue]
	return nil
}

func (id *internalDriver) UnSubscribe(topic, queue, routeKey string) error {
	delete(id.relation[topic][queue], routeKey)
	return nil
}

func (id *internalDriver) SendToQueue(queue string, content []byte, delay time.Duration) error {
	id.queues[queue].msgChan <- internalData{delay: delay, data: content}
	return nil
}

func (id *internalDriver) SendToTopic(topic string, content []byte, routeKey string) error {
	for _, queues := range id.relation[topic] {
		for rk, queue := range queues {
			if rk == routeKey {
				queue.msgChan <- internalData{delay: queue.delay, data: content}
			}
		}
	}
	return nil
}

func (id *internalDriver) ReceiveMessage(ctx context.Context, queue string, errChan chan error, handler func([]byte) bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-id.queues[queue].msgChan:
			utils.Goroutine(func() {
				if msg.delay > 0 {
					<-time.NewTimer(msg.delay).C
				}
				if handler(msg.data) == false {
					_ = id.SendToQueue(queue, msg.data, msg.delay)
				}
			})
		}
	}
}
