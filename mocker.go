package bus

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

type mockDriver struct {
	mock.Mock
	itd *internalDriver
}

func (m *mockDriver) CreateQueue(name string, delay time.Duration) error {
	err := m.Called(name, delay).Error(0)
	if err == nil {
		err = m.itd.CreateQueue(name, delay)
	}
	return err
}

func (m *mockDriver) CreateTopic(name string) error {
	err := m.Called(name).Error(0)
	if err == nil {
		err = m.itd.CreateTopic(name)
	}
	return err
}

func (m *mockDriver) Subscribe(topic, queue, routeKey string) error {
	err := m.Called(topic, queue, routeKey).Error(0)
	if err == nil {
		err = m.itd.Subscribe(topic, queue, routeKey)
	}
	return err
}

func (m *mockDriver) UnSubscribe(topic, queue, routeKey string) error {
	err := m.Called(topic, queue, routeKey).Error(0)
	if err == nil {
		err = m.itd.UnSubscribe(topic, queue, routeKey)
	}
	return err
}

func (m *mockDriver) SendToQueue(queue string, content []byte, delay time.Duration) error {
	err := m.Called(queue, content, delay).Error(0)
	if err == nil {
		err = m.itd.SendToQueue(queue, content, delay)
	}
	return err
}

func (m *mockDriver) SendToTopic(topic string, content []byte, routeKey string) error {
	err := m.Called(topic, content, routeKey).Error(0)
	if err == nil {
		err = m.itd.SendToTopic(topic, content, routeKey)
	}
	return err
}

func (m *mockDriver) ReceiveMessage(ctx context.Context, queue string, errChan chan error, handler func([]byte) bool) {
	m.itd.ReceiveMessage(ctx, queue, errChan, handler)
}
