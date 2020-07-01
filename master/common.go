package master

// The number of shards.
const NShards = 10

type Config struct {
	Num    int          // config number
	Shards [NShards]int // shard -> gid
	Groups map[int]int  // gid -> peers
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int]int // new GID -> peers

	RID int
	CID int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	RID int
	CID int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number

	RID int
	CID int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
