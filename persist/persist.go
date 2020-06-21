package persist

type Persister interface {
	SaveStateAndSnapshot(data []byte, snapshot []byte)
	SaveRaftState(data []byte)
	RaftStateSize() int
	ReadSnapshot() []byte
	ReadRaftState() []byte
	SnapshotSize() int
}
