package persist

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/artor1os/dkv/util"
)

type Persister interface {
	SaveStateAndSnapshot(data []byte, snapshot []byte)
	SaveState(data []byte)
	StateSize() int
	ReadSnapshot() []byte
	ReadState() []byte
	SnapshotSize() int
}

const stateFile = "state"
const snapshotFile = "snapshot"

type fs struct {
	mu sync.Mutex

	dir string
}

func (p *fs) openFile(f string) *os.File {
	fileName := path.Join(p.dir, f)
	sf, err := os.Open(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			sf, err = os.Create(fileName)
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	return sf
}

func (p *fs) SaveStateAndSnapshot(data []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sf := p.openFile(stateFile)
	snf := p.openFile(snapshotFile)
	if err := util.Sequential(func() error {
		return util.Sequential(func() error {
			_, err := snf.Write(snapshot)
			return err
		}, snf.Close)
	}, func() error {
		return util.Sequential(func() error {
			_, err := sf.Write(data)
			return err
		}, sf.Close)
	}); err != nil {
		panic(err)
	}
}

func (p *fs) SaveState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sf := p.openFile(stateFile)
	if err := util.Sequential(func() error {
		_, err := sf.Write(data)
		return err
	}, sf.Close); err != nil {
		panic(err)
	}
}

func (p *fs) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := ioutil.ReadFile(path.Join(p.dir, stateFile))
	if err != nil {
		panic(err)
	}
	return b
}

func (p *fs) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := ioutil.ReadFile(path.Join(p.dir, snapshotFile))
	if err != nil {
		panic(err)
	}
	return b
}

func (p *fs) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := os.Stat(path.Join(p.dir, stateFile))
	if err != nil {
		panic(err)
	}
	return int(s.Size())
}

func (p *fs) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := os.Stat(path.Join(p.dir, snapshotFile))
	if err != nil {
		panic(err)
	}
	return int(s.Size())
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

func (p *memory) SaveState(data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = data
}

func (p *memory) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

func (p *memory) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshot
}

func (p *memory) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

func (p *memory) SnapshotSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}

func New(dir string) Persister {
	return &memory{}
}
