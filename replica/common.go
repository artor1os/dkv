package replica

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RID   int
	CID   int64
}

type PutAppendReply struct {
	Err Err
}

type GetDeleteArgs struct {
	Key string
	Op string // "Get" or "Remove"
	RID int
	CID int64
}

type GetDeleteReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Shard         int
	Data          map[string]string
	Num           int
	LastCommitted map[int64]int
}

type MigrateReply struct {
	Err Err
}
