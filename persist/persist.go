package persist

type Persister interface {
	SaveStateAndSnapshot(data []byte, snapshot []byte)
	SaveRaftState(data []byte)
	RaftStateSize() int
	ReadSnapshot() []byte
	ReadRaftState() []byte
	SnapshotSize() int
}

type persister struct {
}

func (p *persister) SaveStateAndSnapshot(data []byte, snapshot []byte) {

}

func (p *persister) SaveRaftState(data []byte) {

}

func (p *persister) RaftStateSize() int {
	return 0
}

func (p *persister) ReadSnapshot() []byte {
	return nil
}

func (p *persister) ReadRaftState() []byte {
	return nil
}

func (p *persister) SnapshotSize() int {
	return 0
}

func New(dir string) Persister {
	return &persister{}
}
