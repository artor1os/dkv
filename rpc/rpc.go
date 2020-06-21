package rpc

type Endpoint interface {
	Call(string, interface{}, interface{}) bool
}
