package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// import "6.824/src/shardmaster"

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWaiting       = "ErrWaiting"
	ErrInTransaction = "ErrInTransaction"
	ErrInPrepare     = "ErrInPrepare"
	ErrChanged       = "ErrChanged"
	ErrConfigChanged = "ErrConfigChanged"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key                string
	Value              string
	Op                 string // "Put" or "Append"
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionNum     int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key                string
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionNum     int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type PrepareArgs struct {
	Key                string
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionNum     int
}

type PrepareReply struct {
	WrongLeader bool
	Err         Err
	Prepared    bool
}

type CommitArgs struct {
	Key                string
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionNum     int
}

type CommitReply struct {
	WrongLeader bool
	Err         Err
	Done        bool
}

type AbortArgs struct {
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionNum     int
}

type AbortReply struct {
	WrongLeader bool
	Done        bool
}

type DeleteShardDataArgs struct {
	ConfigNum int
	Shard     int
	Gid       int
}

type DeleteShardDataReply struct {
	Err Err
}

type RequestShardDataArgs struct {
	ConfigNum int
	Shard     int
	Gid       int
}

type RequestShardDataReply struct {
	Err            Err
	ConfigNum      int
	Shard          int
	Data           map[string]string
	LatestOpSeqNum map[int64]int
}
