package persist

import (
	"sync"
)

type Persister interface {
	SaveStateAndSnapshot(data []byte, snapshot []byte)
	SaveRaftState(data []byte)
	RaftStateSize() int
	ReadSnapshot() []byte
	ReadRaftState() []byte
	SnapshotSize() int
}

type memory struct {
	mu sync.Mutex

	state    []byte
	snapshot []byte
}

func (p *memory) SaveStateAndSnapshot(data []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = data
	p.snapshot = snapshot
}

func (p *memory) SaveRaftState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = data
}

func (p *memory) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

func (p *memory) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}

func (p *memory) ReadRaftState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

func (p *memory) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}

func New(dir string) Persister {
	return &memory{}
}
