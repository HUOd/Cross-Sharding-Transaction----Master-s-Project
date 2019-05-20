package transactionmanager

// import "cst/src/shardkv"

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrNotActive      = "ErrNotActive"
	ErrAbort          = "ErrAbort"
	ErrConfigChanged  = "ErrConfigChanged"
	ErrDiplicateState = "ErrDuplicateState"
)

type Err string

// Get
type GetArgs struct {
	Key                string
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionID      int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

// Put or Append
type PutAppendArgs struct {
	Key                string
	Value              string
	Op                 string // "Put" or "Append"
	ClientID           int64
	ClientLastOpSeqNum int
	TransactionID      int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

// Begin Transaction
type BeginArgs struct {
	ClientID           int64
	ClientLastOpSeqNum int
}

type BeginReply struct {
	TransactionID int
	Err           Err
	WrongLeader   bool
}

// Commit Transaction
type CommitArgs struct {
	TransactionID      int
	ClientID           int64
	ClientLastOpSeqNum int
}

type CommitReply struct {
	Err         Err
	WrongLeader bool
}

// Abort Transaction
type AbortArgs struct {
	TransactionID      int
	ClientID           int64
	ClientLastOpSeqNum int
}

type AbortReply struct {
	Err         Err
	WrongLeader bool
}

type UpdateArgs struct {
	TransactionID      int
	Key                string
	Gid                int
	ClientID           int64
	ClientLastOpSeqNum int
}

type UpdateReply struct {
	Err         Err
	WrongLeader bool
}

type UpdateStateArgs struct {
	TransactionID      int
	State              TsState
	ClientID           int64
	ClientLastOpSeqNum int
}

type UpdateStateReply struct {
	Err         Err
	WrongLeader bool
}
