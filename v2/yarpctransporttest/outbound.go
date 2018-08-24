// Code generated by MockGen. DO NOT EDIT.
// Source: go.uber.org/yarpc/v2/yarpctransport (interfaces: UnaryOutbound,StreamOutbound)

// Package yarpctransporttest is a generated GoMock package.
package yarpctransporttest

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	yarpctransport "go.uber.org/yarpc/v2/yarpctransport"
	reflect "reflect"
)

// MockUnaryOutbound is a mock of UnaryOutbound interface
type MockUnaryOutbound struct {
	ctrl     *gomock.Controller
	recorder *MockUnaryOutboundMockRecorder
}

// MockUnaryOutboundMockRecorder is the mock recorder for MockUnaryOutbound
type MockUnaryOutboundMockRecorder struct {
	mock *MockUnaryOutbound
}

// NewMockUnaryOutbound creates a new mock instance
func NewMockUnaryOutbound(ctrl *gomock.Controller) *MockUnaryOutbound {
	mock := &MockUnaryOutbound{ctrl: ctrl}
	mock.recorder = &MockUnaryOutboundMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockUnaryOutbound) EXPECT() *MockUnaryOutboundMockRecorder {
	return m.recorder
}

// Call mocks base method
func (m *MockUnaryOutbound) Call(arg0 context.Context, arg1 *yarpctransport.Request) (*yarpctransport.Response, error) {
	ret := m.ctrl.Call(m, "Call", arg0, arg1)
	ret0, _ := ret[0].(*yarpctransport.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Call indicates an expected call of Call
func (mr *MockUnaryOutboundMockRecorder) Call(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Call", reflect.TypeOf((*MockUnaryOutbound)(nil).Call), arg0, arg1)
}

// MockStreamOutbound is a mock of StreamOutbound interface
type MockStreamOutbound struct {
	ctrl     *gomock.Controller
	recorder *MockStreamOutboundMockRecorder
}

// MockStreamOutboundMockRecorder is the mock recorder for MockStreamOutbound
type MockStreamOutboundMockRecorder struct {
	mock *MockStreamOutbound
}

// NewMockStreamOutbound creates a new mock instance
func NewMockStreamOutbound(ctrl *gomock.Controller) *MockStreamOutbound {
	mock := &MockStreamOutbound{ctrl: ctrl}
	mock.recorder = &MockStreamOutboundMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockStreamOutbound) EXPECT() *MockStreamOutboundMockRecorder {
	return m.recorder
}

// CallStream mocks base method
func (m *MockStreamOutbound) CallStream(arg0 context.Context, arg1 *yarpctransport.StreamRequest) (*yarpctransport.ClientStream, error) {
	ret := m.ctrl.Call(m, "CallStream", arg0, arg1)
	ret0, _ := ret[0].(*yarpctransport.ClientStream)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CallStream indicates an expected call of CallStream
func (mr *MockStreamOutboundMockRecorder) CallStream(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CallStream", reflect.TypeOf((*MockStreamOutbound)(nil).CallStream), arg0, arg1)
}
