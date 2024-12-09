package gooutstore

import (
	"context"
	"reflect"
	"testing"
)

type clientMock struct {
	t *testing.T

	callers map[string]*caller
	expects map[string]*expect
}

func NewClientMock(t *testing.T) *clientMock {
	const methodsCount = 6
	c := &clientMock{t: t, callers: make(map[string]*caller, methodsCount), expects: make(map[string]*expect, methodsCount)}

	return c
}

func (c *clientMock) Create(ctx context.Context, messages []Message) error {
	methodName := "Create"
	args := make([]interface{}, 2)
	args[0] = ctx
	args[1] = messages

	err, _ := c.call(methodName, args...).res[0].(error)
	return err
}

func (c *clientMock) ReadBatch(ctx context.Context, messageType string, batchSize int) ([]Message, error) {
	methodName := "ReadBatch"
	args := make([]interface{}, 3)
	args[0] = ctx
	args[1] = messageType
	args[2] = batchSize

	expect := c.call(methodName, args...)
	messages, _ := expect.res[0].([]Message)
	err, _ := expect.res[1].(error)
	return messages, err
}

func (c *clientMock) SetDone(ctx context.Context, m Message) error {
	methodName := "SetDone"
	args := make([]interface{}, 2)
	args[0] = ctx
	args[1] = m

	expect := c.call(methodName, args...)
	err, _ := expect.res[0].(error)
	return err
}

func (c *clientMock) IncRetry(ctx context.Context, m Message) error {
	methodName := "IncRetry"
	args := make([]interface{}, 2)
	args[0] = ctx
	args[1] = m

	expect := c.call(methodName, args...)
	err, _ := expect.res[0].(error)
	return err
}

func (c *clientMock) SetBroken(ctx context.Context, m Message) error {
	methodName := "SetBroken"
	args := make([]interface{}, 2)
	args[0] = ctx
	args[1] = m

	expect := c.call(methodName, args...)
	err, _ := expect.res[0].(error)
	return err
}

func (c *clientMock) call(method string, args ...interface{}) *expect {
	expect, ok := c.expects[method]
	if !ok {
		c.t.Fatalf("unexpected call %s", method)
	}

	if len(expect.res) < 1 {
		c.t.Fatalf("no result values for %s", method)
	}

	cal, ok := c.callers[method]
	if !ok {
		c.callers[method] = &caller{args: args, count: 1}
	} else {
		cal.count++
	}

	return expect
}

func (c *clientMock) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (c *clientMock) on(funcName string, count int, args ...interface{}) *expect {
	c.expects[funcName] = &expect{count: count, args: args}

	return c.expects[funcName]
}

func (e *expect) andReturn(args ...interface{}) {
	e.res = args
}

func (c *clientMock) assert() {
	for funcName, expect := range c.expects {
		if caller, ok := c.callers[funcName]; ok {
			if caller.count != expect.count {
				c.t.Errorf("unexpected call count for %s, got %d, want %d", funcName, caller.count, expect.count)
			}

			for i, arg := range expect.args {
				if !reflect.DeepEqual(caller.args[i], arg) {
					c.t.Errorf("unexpected call args for %s, got %v, want %v", funcName, caller.args[i], arg)
				}
			}
		} else {
			c.t.Errorf("unexpected call %s, but", funcName)
		}
	}
}

type caller struct {
	args  []interface{}
	count int
}

type expect struct {
	args  []interface{}
	res   []interface{}
	count int
}
