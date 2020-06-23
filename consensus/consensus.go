package consensus

import (
	"sync"
)

type Consensus interface {
	Start(interface{}) (int, int, bool)
}

type trivial struct {
	applyCh chan ApplyMsg

	mu sync.Mutex
	index int
}

func NewTrivial(applyCh chan ApplyMsg) Consensus {
	return &trivial{applyCh:applyCh, index:1}
}

func (t *trivial) Start(command interface{}) (int, int, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.applyCh <- ApplyMsg{
		CommandValid:  true,
		Command:       command,
		CommandIndex:  t.index,
	}
	index := t.index
	t.index++
	return index, 0, true
}