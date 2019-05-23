package shardkv

// Some of the algorithm or code logic in this file comes from the resource from internet:
// https://github.com/wqlin/mit-6.824-2018/blob/solution/src/shardkv/server.go

import "cst/src/shardmaster"
import "cst/src/labrpc"
import "cst/src/raft"
import "sync"
import "cst/src/labgob"
import "log"
import "time"
import "bytes"
import "fmt"

// import "strconv"

const ( // Client Op Name
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

const ( // Transaction Op Name
	PREPARE = "Prepare"
	COMMIT  = "Commit"
	ABORT   = "Abort"
)

const ( // Op struct Op type
	CLINET_OP      = "ClientOp"
	SENDING_SHARD  = "SendingShard"
	RECEIVED_SHARD = "ReceivingShard"
	NEW_CONFIG     = "NewConfig"
	TRANSACTION    = "Transaction"
)

const ( // shard state for each shard group
	UNSERVE = iota
	SERVE
	RECEIVING
	SENDING
)

const TransactionTimeOut = 5 * time.Minute

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType        string              // ClientOp / SendingShard / ReceivingShard / NewConfig
	ClientOp      ClientOpStruct      // struct for ClientOp
	NewConfig     shardmaster.Config  // new config for NewConfig
	SendingShard  SendingShardStruct  // for deleting the local shard
	ReceivedShard ReceivedShardStruct // for install a new shard
	Transaction   TransactionStruct   // for transaction
}

type ClientOpStruct struct {
	OpName         string
	ClientID       int64
	ClientOpSeqNum int
	ConfigNum      int
	Key            string
	Value          string
	TransactionNum int
}

type SendingShardStruct struct {
	Shard     int
	ConfigNum int
}

type ReceivedShardStruct struct {
	Shard             int
	ConfigNum         int
	Data              map[string]string
	LastOpSeqNum      map[int64]int
	ShadowData        map[int]map[string]string
	ShadowDataCompare map[int]map[string]string
	PreparedKeys      map[string]int
}

type TransactionStruct struct {
	OpName         string
	ClientID       int64
	ClientOpSeqNum int
	Key            string
	TransactionNum int
}

type OpReply struct {
	Err   Err
	value string
}

type Decision struct {
	TransactionID int
	Key           string
	Op            string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	Rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// common server's functional part
	Killed          bool
	KillChan        chan struct{}
	configTimer     *time.Timer
	receivingTimer  *time.Timer
	ReceivingShards map[int]int // set for knowing which shard are receiving
	// The idea of creating a PendingData space idea comes from: https://github.com/wqlin/mit-6.824-2018/blob/solution/src/shardkv/server.go
	PendingData             map[int]map[int]map[string]string // store the previous data from previous config  configNum -> shard -> key-value
	PendingOpSeqNum         map[int]map[int]map[int64]int     // store the clientOp num from previous config  configNum -> shard -> clientID-clientSeqNum
	shardMaster             *shardmaster.Clerk
	ApplyingConfigNum       int
	ShardStatus             [shardmaster.NShards]int
	LogIndexCallbackChanMap map[int]chan OpReply      // Call back channel
	LocalData               map[int]map[string]string // Primary data store
	LatestClientOpSeqNum    map[int]map[int64]int

	// Transactions
	TransactionTimeoutTimer  *time.Timer
	ShadowDataCompare        map[int]map[int]map[string]string // transactionnum->shard->kv
	ShadowData               map[int]map[int]map[string]string // transactionnum->shard->kv
	PreparedKeys             map[string]int
	PendingShadowDataCompare map[int]map[int]map[int]map[string]string // configNum->transactionnum->shard->kv
	PendingShadowData        map[int]map[int]map[int]map[string]string // configNum->transactionnum->shard->kv
	PendingPreparedKeys      map[int]map[string]int
	DecisionQueue            []Decision // Decision queue for prepare state
	TransactionTimers        map[int]*time.Timer
}

func copyStrStrMap(m map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyInt64IntMap(m map[int64]int) map[int64]int {
	res := make(map[int64]int)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyStrIntMap(m map[string]int) map[string]int {
	res := make(map[string]int)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyIntSet(m map[int]bool) map[int]bool {
	res := make(map[int]bool)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyStrIntSetMap(m map[string]map[int]bool) map[string]map[int]bool {
	res := make(map[string]map[int]bool)
	for k, v := range m {
		res[k] = copyIntSet(v)
	}
	return res
}

func copyConfig(newConfig shardmaster.Config) shardmaster.Config {
	c := shardmaster.Config{}
	c.Num = newConfig.Num
	copy(c.Shards[:], newConfig.Shards[:])

	newConfig.Groups = make(map[int][]string)
	for k, v := range newConfig.Groups {
		c.Groups[k] = v
	}

	return c
}

// Get request, non-transaction Operation has TransactionNum 0
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("ShardKV %d at %d received Get Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
	shard := key2shard(args.Key)
	if _, isLeader := kv.Rf.GetState(); isLeader {
		kv.mu.Lock()
		thisOperation := Op{
			OpType: CLINET_OP,
			ClientOp: ClientOpStruct{
				OpName:         GET,
				ConfigNum:      kv.ApplyingConfigNum,
				ClientID:       args.ClientID,
				ClientOpSeqNum: args.ClientLastOpSeqNum,
				Key:            args.Key,
				TransactionNum: args.TransactionNum,
			},
		}
		kv.mu.Unlock()

		kv.mu.Lock()
		if kv.ShardStatus[shard] == SERVE {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.Unlock()

		if reply.Err != OK {
			DPrintf("ShardKV %d at %d return Get Request from Client %d, Err %v. \n", kv.me, kv.gid, args.ClientID, reply.Err)
			return
		}

		newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

		if isLeader {
			DPrintf("ShardKV %d at %d start a Get request at index %d. \n", kv.me, kv.gid, newIndex)
			kv.mu.Lock()
			mChan := make(chan OpReply)
			kv.LogIndexCallbackChanMap[newIndex] = mChan
			kv.mu.Unlock()

			for {
				select {
				case opreply := <-mChan:
					if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
						if opreply.Err == OK {
							if args.TransactionNum > 0 {
								kv.mu.Lock()
								if kv.ShardStatus[shard] == SERVE {
									kv.setUpShadow(args.Key, args.TransactionNum)
									if value, prs := kv.ShadowData[args.TransactionNum][shard][args.Key]; prs {
										reply.Value = value
										reply.Err = OK
									} else {
										reply.Err = ErrNoKey
									}
								} else {
									reply.Err = ErrWrongGroup
								}
								kv.mu.Unlock()
							} else {
								kv.mu.Lock()
								if kv.ShardStatus[shard] == SERVE {
									if value, prs := kv.LocalData[shard][args.Key]; prs {
										reply.Value = value
										reply.Err = OK
									} else {
										reply.Err = ErrNoKey
									}
								} else {
									reply.Err = ErrWrongGroup
								}
								kv.mu.Unlock()
							}
						} else {
							reply.Err = ErrWrongGroup
						}

						if reply.Err != OK {
							DPrintf("ShardKV %d at %d return Get Request from Client %d for index %d, Err %v. \n", kv.me, kv.gid, args.ClientID, newIndex, reply.Err)
						} else {
							DPrintf("ShardKV %d at %d return Get Request from Client %d for index %d, Err %v. \n", kv.me, kv.gid, args.ClientID, newIndex, reply.Err)
						}
						return
					} else {
						DPrintf("ShardKV %d at %d is not leader #3, reject Get Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						reply.WrongLeader = true
					}
					return
				case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
					if _, isLeader := kv.Rf.GetState(); !isLeader {
						kv.mu.Lock()
						delete(kv.LogIndexCallbackChanMap, newIndex)
						kv.mu.Unlock()
						DPrintf("ShardKV %d at %d  is not leader #4, reject Get Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						reply.WrongLeader = true
						return
					}
				}
			}
		} else {
			DPrintf("ShardKV %d at %d is not leader #2, reject Get Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
			reply.WrongLeader = true
		}
	} else {
		DPrintf("ShardKV %d at %d is not leader #1, reject Get Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
		reply.WrongLeader = true
	}
}

// PutAppend request, non-transaction Operation has TransactionNum 0
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("ShardKV %d at %d received %v Request from Client %d. \n", kv.me, kv.gid, args.Op, args.ClientID)
	if _, isLeader := kv.Rf.GetState(); isLeader {
		kv.mu.Lock()
		thisOperation := Op{
			OpType: CLINET_OP,
			ClientOp: ClientOpStruct{
				OpName:         args.Op,
				ClientID:       args.ClientID,
				ClientOpSeqNum: args.ClientLastOpSeqNum,
				ConfigNum:      kv.ApplyingConfigNum,
				Key:            args.Key,
				Value:          args.Value,
				TransactionNum: args.TransactionNum,
			},
		}
		kv.mu.Unlock()

		shard := key2shard(args.Key)
		kv.mu.Lock()
		if kv.ShardStatus[shard] == SERVE {
			reply.Err = OK
			if args.TransactionNum == 0 {
				if kv.checkIsInTransaction(args.Key) {
					reply.Err = ErrInTransaction
				}
			}
		} else {
			reply.Err = ErrWrongGroup
		}
		kv.mu.Unlock()

		if reply.Err != OK {
			DPrintf("ShardKV %d at %d return %v Request from Client %d, Err %v. \n", kv.me, kv.gid, args.Op, args.ClientID, reply.Err)
			return
		}

		newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

		if isLeader {
			DPrintf("ShardKV %d at %d start a %v request at index %d. \n", kv.me, kv.gid, args.Op, newIndex)
			kv.mu.Lock()
			mChan := make(chan OpReply)
			kv.LogIndexCallbackChanMap[newIndex] = mChan
			kv.mu.Unlock()

			for {

				select {
				case opreply := <-mChan:
					if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
						if opreply.Err == OK {
							reply.Err = OK
						} else {
							reply.Err = opreply.Err
						}

						if reply.Err != OK {
							DPrintf("ShardKV %d at %d return %v Request from Client %d, Err %v. \n", kv.me, kv.gid, args.Op, args.ClientID, reply.Err)
						} else {
							DPrintf("ShardKV %d at %d return %v Request from Client %d, Err %v. \n", kv.me, kv.gid, args.Op, args.ClientID, reply.Err)
						}
						return
					} else {
						DPrintf("ShardKV %d at %d is not leader #3, reject %v Request from Client %d. \n", kv.me, kv.gid, args.Op, args.ClientID)
						reply.WrongLeader = true
					}
					return
				case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
					if _, isLeader := kv.Rf.GetState(); !isLeader {
						kv.mu.Lock()
						delete(kv.LogIndexCallbackChanMap, newIndex)
						kv.mu.Unlock()
						reply.WrongLeader = true
						DPrintf("ShardKV %d at %d is not leader #4, reject %v Request from Client %d. \n", kv.me, kv.gid, args.Op, args.ClientID)
						return
					}
				}
			}
		} else {
			DPrintf("ShardKV %d at %d is not leader #2, reject %v Request from Client %d. \n", kv.me, kv.gid, args.Op, args.ClientID)
			reply.WrongLeader = true
		}
	} else {
		DPrintf("ShardKV %d at %d is not leader #1, reject %v Request from Client %d. \n", kv.me, kv.gid, args.Op, args.ClientID)
		reply.WrongLeader = true
	}
}

// Prepare request from transaction manager. Return vote Decision
func (kv *ShardKV) Prepare(args *PrepareArgs, reply *PrepareReply) {
	DPrintf("ShardKV %d at %d received Prepare Request from Client %d for Transaction %d. \n", kv.me, kv.gid, args.ClientID, args.TransactionNum)
	if _, isLeader := kv.Rf.GetState(); isLeader {
		shard := key2shard(args.Key)
		kv.mu.Lock()
		if kv.ShardStatus[shard] == SERVE {
			reply.Err = OK
		} else {
			reply.Err = ErrConfigChanged
			kv.mu.Unlock()
			return
		}

		if tnum, prs := kv.PreparedKeys[args.Key]; prs && tnum != args.TransactionNum {
			reply.Err = ErrInPrepare
			kv.mu.Unlock()
			return
		}

		kv.mu.Unlock()

		if reply.Err != OK {
			DPrintf("ShardKV %d at %d return Prepare Request from Client %d, Err %v. \n", kv.me, kv.gid, args.ClientID, reply.Err)
			return
		}

		thisOperation := Op{
			OpType: TRANSACTION,
			Transaction: TransactionStruct{
				OpName:         PREPARE,
				Key:            args.Key,
				ClientID:       args.ClientID,
				ClientOpSeqNum: args.ClientLastOpSeqNum,
				TransactionNum: args.TransactionNum,
			},
		}

		newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

		if isLeader {
			DPrintf("ShardKV %d at %d start a Prepare request at index %d. \n", kv.me, kv.gid, newIndex)
			kv.mu.Lock()
			mChan := make(chan OpReply)
			kv.LogIndexCallbackChanMap[newIndex] = mChan
			kv.mu.Unlock()

			for {
				select {
				case opreply := <-mChan:
					if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
						if opreply.Err == OK {
							reply.Err = OK
						} else {
							reply.Err = opreply.Err
						}

						if reply.Err != OK {
							DPrintf("ShardKV %d at %d return Prepare Request from Client %d, Err %v. \n", kv.me, kv.gid, args.ClientID, reply.Err)
						} else {
							DPrintf("ShardKV %d at %d return Prepare Request from Client %d, Err %v. \n", kv.me, kv.gid, args.ClientID, reply.Err)
						}
					} else {
						DPrintf("ShardKV %d at %d is not leader #3, reject Prepare Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						reply.WrongLeader = true
					}
					return
				case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
					if _, isLeader := kv.Rf.GetState(); !isLeader {
						kv.mu.Lock()
						delete(kv.LogIndexCallbackChanMap, newIndex)
						kv.mu.Unlock()
						reply.WrongLeader = true
						DPrintf("ShardKV %d at %d is not leader #4, reject Prepare Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						return
					}
				}
			}
		} else {
			DPrintf("ShardKV %d at %d is not leader #2, reject Prepare Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
			reply.WrongLeader = true
		}
	} else {
		DPrintf("ShardKV %d at %d is not leader #1, reject Prepare Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
		reply.WrongLeader = true
	}
}

//Commit request from Transaction manager. Return Done
func (kv *ShardKV) Commit(args *CommitArgs, reply *CommitReply) {
	DPrintf("ShardKV %d at %d received Commit Request from Client %d for Transaction %d. \n", kv.me, kv.gid, args.ClientID, args.TransactionNum)
	if _, isLeader := kv.Rf.GetState(); isLeader {

		thisOperation := Op{
			OpType: TRANSACTION,
			Transaction: TransactionStruct{
				OpName:         COMMIT,
				Key:            args.Key,
				ClientID:       args.ClientID,
				ClientOpSeqNum: args.ClientLastOpSeqNum,
				TransactionNum: args.TransactionNum,
			},
		}

		newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

		if isLeader {
			DPrintf("ShardKV %d at %d start a Commit request at index %d. \n", kv.me, kv.gid, newIndex)
			kv.mu.Lock()
			mChan := make(chan OpReply)
			kv.LogIndexCallbackChanMap[newIndex] = mChan
			kv.mu.Unlock()

			for {
				select {
				case <-mChan:
					if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
						reply.Err = OK
						reply.Done = true
						DPrintf("ShardKV %d at %d Commit Request from Client %d Done. \n", kv.me, kv.gid, args.ClientID)
					} else {
						DPrintf("ShardKV %d at %d is not leader #3, reject Commit Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						reply.WrongLeader = true
					}
					return
				case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
					if _, isLeader := kv.Rf.GetState(); !isLeader {
						kv.mu.Lock()
						delete(kv.LogIndexCallbackChanMap, newIndex)
						kv.mu.Unlock()
						reply.WrongLeader = true
						DPrintf("ShardKV %d at %d is not leader #4, reject Commit Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						return
					}
				}
			}
		} else {
			DPrintf("ShardKV %d at %d is not leader #2, reject Commit Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
			reply.WrongLeader = true
		}
	} else {
		DPrintf("ShardKV %d at %d is not leader #1, reject Commit Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
		reply.WrongLeader = true
	}
}

//Abort request from Transaction manager. Return Done
func (kv *ShardKV) Abort(args *AbortArgs, reply *AbortReply) {
	DPrintf("ShardKV %d at %d received Abort Request from Client %d for Transaction %d. \n", kv.me, kv.gid, args.ClientID, args.TransactionNum)
	if _, isLeader := kv.Rf.GetState(); isLeader {

		thisOperation := Op{
			OpType: TRANSACTION,
			Transaction: TransactionStruct{
				OpName:         ABORT,
				Key:            args.Key,
				ClientID:       args.ClientID,
				ClientOpSeqNum: args.ClientLastOpSeqNum,
				TransactionNum: args.TransactionNum,
			},
		}

		newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

		if isLeader {
			DPrintf("ShardKV %d at %d start a Abort request at index %d. \n", kv.me, kv.gid, newIndex)
			kv.mu.Lock()
			mChan := make(chan OpReply)
			kv.LogIndexCallbackChanMap[newIndex] = mChan
			kv.mu.Unlock()

			for {
				select {
				case <-mChan:
					if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
						reply.Done = true
						DPrintf("ShardKV %d at %d Abort Request for Transaction %d Done. \n", kv.me, kv.gid, args.TransactionNum)
					} else {
						DPrintf("ShardKV %d at %d is not leader #3, reject Abort Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						reply.WrongLeader = true
					}
					return
				case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
					if _, isLeader := kv.Rf.GetState(); !isLeader {
						kv.mu.Lock()
						delete(kv.LogIndexCallbackChanMap, newIndex)
						kv.mu.Unlock()
						reply.WrongLeader = true
						DPrintf("ShardKV %d at %d is not leader #4, reject Abort Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
						return
					}
				}
			}
		} else {
			DPrintf("ShardKV %d at %d is not leader #2, reject Abort Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
			reply.WrongLeader = true
		}
	} else {
		DPrintf("ShardKV %d at %d is not leader #1, reject Abort Request from Client %d. \n", kv.me, kv.gid, args.ClientID)
		reply.WrongLeader = true
	}
}

// Add a newconfig and broadcastToFollowers
func (kv *ShardKV) registerNewConfig(NewConfig shardmaster.Config) {

	// kv.mu.Lock()
	// if kv.ApplyingConfigNum >= NewConfig.Num {
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()

	thisOperation := Op{
		OpType:    NEW_CONFIG,
		NewConfig: copyConfig(NewConfig),
	}

	newIndex, _, isLeader := kv.Rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardKV %d at %d registered a new config at %d with config num %d. \n", kv.me, kv.gid, newIndex, NewConfig.Num)
	}
}

// start delete the pending data in shard group
func (kv *ShardKV) StartSendSucceed(args *DeleteShardDataArgs) bool {

	kv.mu.Lock()
	thisOperation := Op{
		OpType: SENDING_SHARD,
		SendingShard: SendingShardStruct{
			Shard:     args.Shard,
			ConfigNum: args.ConfigNum,
		},
	}
	kv.mu.Unlock()

	newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardKV %d at %d start a send succeed Op at index %d for shard %d. \n", kv.me, kv.gid, newIndex, args.Shard)
		kv.mu.Lock()
		mChan := make(chan OpReply)
		kv.LogIndexCallbackChanMap[newIndex] = mChan
		kv.mu.Unlock()

		timeOut := time.NewTimer(2 * time.Second)
		for {
			select {
			case opreply := <-mChan:
				if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
					if opreply.Err == OK {
						return true
					}
				}
				return false
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := kv.Rf.GetState(); !isLeader {
					kv.mu.Lock()
					delete(kv.LogIndexCallbackChanMap, newIndex)
					kv.mu.Unlock()
					return false
				}
			case <-timeOut.C:
				kv.mu.Lock()
				delete(kv.LogIndexCallbackChanMap, newIndex)
				kv.mu.Unlock()
				return false
			}

		}
	}

	return false
}

// start receiving the pending data in shard group
func (kv *ShardKV) StartReceivedSucceed(args *RequestShardDataReply) bool {

	kv.mu.Lock()

	thisOperation := Op{
		OpType: RECEIVED_SHARD,
		ReceivedShard: ReceivedShardStruct{
			Shard:             args.Shard,
			ConfigNum:         args.ConfigNum,
			Data:              copyStrStrMap(args.Data),
			LastOpSeqNum:      copyInt64IntMap(args.LatestOpSeqNum),
			ShadowData:        make(map[int]map[string]string),
			ShadowDataCompare: make(map[int]map[string]string),
			PreparedKeys:      copyStrIntMap(args.PreparedKeys),
		},
	}

	for tnum, m := range args.ShadowData {
		thisOperation.ReceivedShard.ShadowData[tnum] = copyStrStrMap(m)
	}

	for tnum, m := range args.ShadowDataCompare {
		thisOperation.ReceivedShard.ShadowDataCompare[tnum] = copyStrStrMap(m)
	}

	kv.mu.Unlock()

	newIndex, thisTerm, isLeader := kv.Rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardKV %d at %d start a received succeed Op at index %d for shard %d. \n", kv.me, kv.gid, newIndex, args.Shard)
		kv.mu.Lock()
		mChan := make(chan OpReply)
		kv.LogIndexCallbackChanMap[newIndex] = mChan
		kv.mu.Unlock()

		timeOut := time.NewTimer(2 * time.Second)
		for {
			select {
			case opreply := <-mChan:
				if curTerm, isLeader := kv.Rf.GetState(); isLeader && curTerm == thisTerm {
					if opreply.Err == OK {
						return true
					}
				}
				return false
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := kv.Rf.GetState(); !isLeader {
					kv.mu.Lock()
					delete(kv.LogIndexCallbackChanMap, newIndex)
					kv.mu.Unlock()
					return false
				}
			case <-timeOut.C:
				kv.mu.Lock()
				delete(kv.LogIndexCallbackChanMap, newIndex)
				kv.mu.Unlock()
				return false
			}
		}
	}

	return false
}

// find the data in pending data, send it to the requester
func (kv *ShardKV) RequestShardData(args *RequestShardDataArgs, reply *RequestShardDataReply) {

	DPrintf("ShardKV %d at %d received RequestShardData RPC from %d for config num %d and shard %d. \n", kv.me, kv.gid, args.Gid, args.ConfigNum, args.Shard)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum < kv.ApplyingConfigNum {
		if _, prs := kv.PendingData[args.ConfigNum]; prs {
			if _, prs := kv.PendingData[args.ConfigNum][args.Shard]; prs {
				reply.Err = OK
				reply.Shard = args.Shard
				reply.ConfigNum = args.ConfigNum
				reply.Data = copyStrStrMap(kv.PendingData[args.ConfigNum][args.Shard])
				reply.LatestOpSeqNum = copyInt64IntMap(kv.PendingOpSeqNum[args.ConfigNum][args.Shard])

				reply.PreparedKeys = make(map[string]int)
				reply.ShadowData = make(map[int]map[string]string)
				reply.ShadowDataCompare = make(map[int]map[string]string)
				if _, prs := kv.PendingShadowData[args.ConfigNum]; prs {
					if _, prs := kv.PendingPreparedKeys[args.ConfigNum]; prs {
						reply.PreparedKeys = copyStrIntMap(kv.PendingPreparedKeys[args.ConfigNum])
					}

					for tnum, data := range kv.PendingShadowData[args.ConfigNum] {
						if _, prs := data[args.Shard]; prs {
							reply.ShadowData[tnum] = copyStrStrMap(kv.PendingShadowData[args.ConfigNum][tnum][args.Shard])
							reply.ShadowDataCompare[tnum] = copyStrStrMap(kv.PendingShadowDataCompare[args.ConfigNum][tnum][args.Shard])
						}
					}
				}

				DPrintf("ShardKV %d at %d reply RequestShardData RPC from %d with OK. \n", kv.me, kv.gid, args.Gid)
				return
			} else {
				DPrintf("ShardKV %d at %d reply RequestShardData RPC from %d with ERR, no such shard num %d in pending data. \n", kv.me, kv.gid, args.Gid, args.Shard)
			}
		} else {
			DPrintf("ShardKV %d at %d reply RequestShardData RPC from %d with ERR, no such config num %d in pending data. \n", kv.me, kv.gid, args.Gid, args.ConfigNum)
		}
	} else {
		DPrintf("ShardKV %d at %d reply RequestShardData RPC from %d with ERR, such config num %d not apply. \n", kv.me, kv.gid, args.Gid, args.ConfigNum)
		kv.PrintShardStatus()
	}
	reply.Err = ErrWrongGroup
	return
}

func (kv *ShardKV) DeleteShardData(args *DeleteShardDataArgs, reply *DeleteShardDataReply) {
	if _, isLeader := kv.Rf.GetState(); isLeader {
		DPrintf("ShardKV %d at %d received DeleteShardData RPC from %d. \n", kv.me, kv.gid, args.Gid)
		kv.mu.Lock()
		if _, prs := kv.PendingData[args.ConfigNum]; prs {
			if _, prs := kv.PendingData[args.ConfigNum][args.Shard]; prs {
				kv.mu.Unlock()
				go kv.StartSendSucceed(args)
				reply.Err = OK
				return
			}
		}

		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	DPrintf("ShardKV %d at %d is not leader, reject InstallShardData RPC. \n", kv.me, kv.gid)
}

// Shard request Receiving shard
func (kv *ShardKV) AddNewShard(newShard int, config shardmaster.Config, mChan chan struct{}) {
	DPrintf("ShardKV %d at %d is trying adding shard %d. at config num %d. \n", kv.me, kv.gid, newShard, kv.ApplyingConfigNum)

	args := &RequestShardDataArgs{
		ConfigNum: config.Num,
		Shard:     newShard,
		Gid:       kv.gid,
	}

	targetGID := config.Shards[newShard]
	names := config.Groups[targetGID]

	for _, name := range names {
		server := kv.make_end(name)
		reply := &RequestShardDataReply{}
		ok := server.Call("ShardKV.RequestShardData", args, reply)
		DPrintf("ShardKV %d at %d sent new RequestShardData RPC to %d. \n", kv.me, kv.gid, targetGID)
		if ok && reply.Err == OK {
			DPrintf("ShardKV %d at %d received RequestShardData RPC reply from %d. \n", kv.me, kv.gid, targetGID)
			kv.StartReceivedSucceed(reply)
			break
		}
	}

	mChan <- struct{}{}
}

func (kv *ShardKV) RemoveAShard(shard int) {
	if _, isLeader := kv.Rf.GetState(); isLeader {

		kv.mu.Lock()
		config := kv.shardMaster.Query(kv.ApplyingConfigNum)
		for config.Num > 1 && config.Shards[shard] == kv.gid {
			config = kv.shardMaster.Query(config.Num - 1)
		}
		kv.mu.Unlock()

		if config.Num == 1 && config.Shards[shard] == kv.gid {
			return
		}

		kv.mu.Lock()
		NewConfig := kv.shardMaster.Query(-1)

		args := &DeleteShardDataArgs{
			ConfigNum: kv.ApplyingConfigNum,
			Shard:     shard,
			Gid:       kv.gid,
		}

		targetGID := config.Shards[shard]
		names := NewConfig.Groups[targetGID]
		kv.mu.Unlock()

		sent := false
		for !sent {
			for _, name := range names {
				server := kv.make_end(name)
				reply := &DeleteShardDataReply{}
				ok := server.Call("ShardKV.DeleteShardData", args, reply)
				DPrintf("ShardKV %d at %d sent new DeleteShardData RPC. \n", kv.me, kv.gid)
				if ok && reply.Err == OK {
					DPrintf("ShardKV %d at %d received DeleteShardData RPC reply from %d. \n", kv.me, kv.gid, targetGID)
					sent = true
					break
				} else if ok && reply.Err == ErrWrongGroup {
					sent = true
					break
				}
			}
		}

	}
}

func (kv *ShardKV) applyNewConfig(NewConfig shardmaster.Config) {
	DPrintf("ShardKV %d at %d try to apply new config %d. \n", kv.me, kv.gid, NewConfig.Num)

	if NewConfig.Num <= kv.ApplyingConfigNum {
		return
	}

	if len(kv.ReceivingShards) > 0 {
		return
	}

	sendtotal := 0
	receTotal := 0

	var preShardStatus [shardmaster.NShards]int
	copy(preShardStatus[:], kv.ShardStatus[:])

	if NewConfig.Num == 1 {
		for shard, gid := range NewConfig.Shards {
			if kv.gid == gid && kv.ShardStatus[shard] == UNSERVE {
				kv.LocalData[shard] = make(map[string]string)
				kv.LatestClientOpSeqNum[shard] = make(map[int64]int)
				kv.ShardStatus[shard] = SERVE
			}
		}
	} else {
		for shard, gid := range NewConfig.Shards {
			if kv.gid == gid && kv.ShardStatus[shard] == UNSERVE {
				kv.ShardStatus[shard] = RECEIVING
				kv.ReceivingShards[shard] = NewConfig.Num - 1
				receTotal++
			} else if kv.gid != gid && kv.ShardStatus[shard] == SERVE {
				kv.ShardStatus[shard] = SENDING
				sendtotal++
			}
		}
	}

	DPrintf("ShardKV %d at %d start apply new config %d, %d receing, %d sending. \n", kv.me, kv.gid, NewConfig.Num, receTotal, sendtotal)

	if sendtotal > 0 {
		if _, prs := kv.PendingData[NewConfig.Num-1]; !prs {
			kv.PendingData[NewConfig.Num-1] = make(map[int]map[string]string)
			kv.PendingOpSeqNum[NewConfig.Num-1] = make(map[int]map[int64]int)
		}

		for i := range kv.ShardStatus {
			if kv.ShardStatus[i] == SENDING {
				kv.PendingData[NewConfig.Num-1][i] = copyStrStrMap(kv.LocalData[i])
				kv.PendingOpSeqNum[NewConfig.Num-1][i] = copyInt64IntMap(kv.LatestClientOpSeqNum[i])
				delete(kv.LocalData, i)
				delete(kv.LatestClientOpSeqNum, i)
				kv.ShardStatus[i] = UNSERVE

				tnums := make([]int, 0)
				for t, v := range kv.ShadowDataCompare {
					if _, prs := v[i]; prs {
						tnums = append(tnums, t)
					}
				}

				if len(tnums) > 0 {
					if _, prs := kv.PendingShadowData[NewConfig.Num-1]; !prs {
						kv.PendingShadowData[NewConfig.Num-1] = make(map[int]map[int]map[string]string)
						kv.PendingShadowDataCompare[NewConfig.Num-1] = make(map[int]map[int]map[string]string)
					}

					for _, tnum := range tnums {
						if _, prs := kv.PendingShadowData[NewConfig.Num-1][tnum]; !prs {
							kv.PendingShadowData[NewConfig.Num-1][tnum] = make(map[int]map[string]string)
							kv.PendingShadowDataCompare[NewConfig.Num-1][tnum] = make(map[int]map[string]string)
						}

						if _, prs := kv.PendingShadowData[NewConfig.Num-1][tnum][i]; !prs {
							kv.PendingShadowData[NewConfig.Num-1][tnum][i] = make(map[string]string)
							kv.PendingShadowDataCompare[NewConfig.Num-1][tnum][i] = make(map[string]string)
						}

						kv.PendingShadowData[NewConfig.Num-1][tnum][i] = kv.ShadowData[tnum][i]
						kv.PendingShadowDataCompare[NewConfig.Num-1][tnum][i] = kv.ShadowDataCompare[tnum][i]
					}

					for _, tnum := range tnums {
						delete(kv.ShadowDataCompare[tnum], i)
						delete(kv.ShadowData[tnum], i)

						if len(kv.ShadowDataCompare[tnum]) == 0 {
							delete(kv.ShadowDataCompare, tnum)
							delete(kv.ShadowData, tnum)
						}
					}
				}

				pkeys := make([]string, 0)
				for key := range kv.PreparedKeys {
					shard := key2shard(key)
					if shard == i {
						if _, prs := kv.PendingPreparedKeys[NewConfig.Num-1]; !prs {
							kv.PendingPreparedKeys[NewConfig.Num-1] = make(map[string]int)
						}

						kv.PendingPreparedKeys[NewConfig.Num-1][key] = kv.PreparedKeys[key]
						pkeys = append(pkeys, key)
					}
				}

				for _, key := range pkeys {
					delete(kv.PreparedKeys, key)
				}
			}
		}
	}

	kv.ApplyingConfigNum = NewConfig.Num

	DPrintf("ShardKV %d at %d update Applying Config at %d. \n", kv.me, kv.gid, NewConfig.Num)
	kv.PrintShardStatus()
}

// Install the received data
func (kv *ShardKV) applyReceivedData(receivedShard ReceivedShardStruct) {
	DPrintf("ShardKV %d at %d applied the received data at shard %d, config num %d. \n", kv.me, kv.gid, receivedShard.Shard, kv.ApplyingConfigNum)
	kv.LocalData[receivedShard.Shard] = copyStrStrMap(receivedShard.Data)
	kv.LatestClientOpSeqNum[receivedShard.Shard] = copyInt64IntMap(receivedShard.LastOpSeqNum)

	for tnum := range receivedShard.ShadowData {
		if _, prs := kv.ShadowData[tnum]; !prs {
			kv.ShadowData[tnum] = make(map[int]map[string]string)
			kv.ShadowDataCompare[tnum] = make(map[int]map[string]string)
		}

		kv.ShadowData[tnum][receivedShard.Shard] = copyStrStrMap(receivedShard.ShadowData[tnum])
		kv.ShadowDataCompare[tnum][receivedShard.Shard] = copyStrStrMap(receivedShard.ShadowDataCompare[tnum])
		kv.TransactionTimers[tnum] = time.NewTimer(TransactionTimeOut)
	}

	for key, tnum := range receivedShard.PreparedKeys {
		kv.PreparedKeys[key] = receivedShard.PreparedKeys[key]
		delete(kv.TransactionTimers, tnum)
	}

	for len(kv.DecisionQueue) > 0 {
		decision := kv.DecisionQueue[0]
		kv.DecisionQueue = kv.DecisionQueue[1:]

		if decision.Op == COMMIT {
			if _, prs := kv.PreparedKeys[decision.Key]; prs {
				delete(kv.PreparedKeys, decision.Key)
				value, prs := kv.ShadowData[decision.TransactionID][receivedShard.Shard][decision.Key]
				if !prs {
					value = ""
				}

				kv.LocalData[receivedShard.Shard][decision.Key] = value

				delete(kv.ShadowData[decision.TransactionID][receivedShard.Shard], decision.Key)
				delete(kv.ShadowDataCompare[decision.TransactionID][receivedShard.Shard], decision.Key)
				if len(kv.ShadowData[decision.TransactionID][receivedShard.Shard]) == 0 {
					delete(kv.ShadowData[decision.TransactionID], receivedShard.Shard)
					delete(kv.ShadowDataCompare[decision.TransactionID], receivedShard.Shard)
					if len(kv.ShadowData[decision.TransactionID]) == 0 {
						delete(kv.ShadowData, decision.TransactionID)
						delete(kv.ShadowDataCompare, decision.TransactionID)
					}
				}
			}

			if _, prs := kv.ShadowData[decision.TransactionID]; prs {
				if _, prs := kv.ShadowData[decision.TransactionID][receivedShard.Shard]; prs {
					if _, prs := kv.ShadowData[decision.TransactionID][receivedShard.Shard][decision.Key]; prs {
						delete(kv.ShadowData[decision.TransactionID][receivedShard.Shard], decision.Key)
						delete(kv.ShadowDataCompare[decision.TransactionID][receivedShard.Shard], decision.Key)
						if len(kv.ShadowData[decision.TransactionID][receivedShard.Shard]) == 0 {
							delete(kv.ShadowData[decision.TransactionID], receivedShard.Shard)
							delete(kv.ShadowDataCompare[decision.TransactionID], receivedShard.Shard)
							if len(kv.ShadowData[decision.TransactionID]) == 0 {
								delete(kv.ShadowData, decision.TransactionID)
								delete(kv.ShadowDataCompare, decision.TransactionID)
							}
						}
					}
				}
			}
		} else if decision.Op == ABORT {
			if _, prs := kv.PreparedKeys[decision.Key]; prs {
				delete(kv.PreparedKeys, decision.Key)

				delete(kv.ShadowData[decision.TransactionID][receivedShard.Shard], decision.Key)
				delete(kv.ShadowDataCompare[decision.TransactionID][receivedShard.Shard], decision.Key)
				if len(kv.ShadowData[decision.TransactionID][receivedShard.Shard]) == 0 {
					delete(kv.ShadowData[decision.TransactionID], receivedShard.Shard)
					delete(kv.ShadowDataCompare[decision.TransactionID], receivedShard.Shard)
					if len(kv.ShadowData[decision.TransactionID]) == 0 {
						delete(kv.ShadowData, decision.TransactionID)
						delete(kv.ShadowDataCompare, decision.TransactionID)
					}
				}
			}

			if _, prs := kv.ShadowData[decision.TransactionID]; prs {
				if _, prs := kv.ShadowData[decision.TransactionID][receivedShard.Shard]; prs {
					if _, prs := kv.ShadowData[decision.TransactionID][receivedShard.Shard][decision.Key]; prs {
						delete(kv.ShadowData[decision.TransactionID][receivedShard.Shard], decision.Key)
						delete(kv.ShadowDataCompare[decision.TransactionID][receivedShard.Shard], decision.Key)
						if len(kv.ShadowData[decision.TransactionID][receivedShard.Shard]) == 0 {
							delete(kv.ShadowData[decision.TransactionID], receivedShard.Shard)
							delete(kv.ShadowDataCompare[decision.TransactionID], receivedShard.Shard)
							if len(kv.ShadowData[decision.TransactionID]) == 0 {
								delete(kv.ShadowData, decision.TransactionID)
								delete(kv.ShadowDataCompare, decision.TransactionID)
							}
						}
					}
				}
			}
		}
	}

	kv.ShardStatus[receivedShard.Shard] = SERVE
	delete(kv.ReceivingShards, receivedShard.Shard)
}

func (kv *ShardKV) applySendingData(sendingShard SendingShardStruct) {
	DPrintf("ShardKV %d at %d applied the sent data at shard %d. \n", kv.me, kv.gid, sendingShard.Shard)
	delete(kv.PendingData[sendingShard.ConfigNum], sendingShard.Shard)
	delete(kv.PendingOpSeqNum[sendingShard.ConfigNum], sendingShard.Shard)

	if len(kv.PendingData[sendingShard.ConfigNum]) == 0 {
		delete(kv.PendingData, sendingShard.ConfigNum)
		delete(kv.PendingOpSeqNum, sendingShard.ConfigNum)
	}
}

func (kv *ShardKV) setUpPrepare(Ts TransactionStruct, reply *PrepareReply) {
	shard := key2shard(Ts.Key)
	if tnum, prs := kv.PreparedKeys[Ts.Key]; prs && tnum == Ts.TransactionNum {
		reply.Err = OK
	} else if prs {
		reply.Err = ErrInPrepare
	} else {
		if kv.ShardStatus[shard] == SERVE {
			lsn, prs := kv.LatestClientOpSeqNum[shard][Ts.ClientID]

			if !prs || (prs && Ts.ClientOpSeqNum > lsn) {
				if shadowingValue, prs := kv.ShadowDataCompare[Ts.TransactionNum][shard][Ts.Key]; prs {
					if value, prs := kv.LocalData[shard][Ts.Key]; prs {
						if shadowingValue == value {
							kv.PreparedKeys[Ts.Key] = Ts.TransactionNum
							reply.Err = OK
							delete(kv.TransactionTimers, Ts.TransactionNum)
						} else {
							reply.Err = ErrChanged
						}
					} else {
						reply.Err = ErrChanged
					}
				} else {
					reply.Err = ErrChanged
				}
				kv.LatestClientOpSeqNum[shard][Ts.ClientID] = Ts.ClientOpSeqNum
			} else {
				if tnum, prs := kv.PreparedKeys[Ts.Key]; prs && tnum == Ts.TransactionNum {
					reply.Err = OK
				} else if prs {
					reply.Err = ErrInPrepare
				} else {
					reply.Err = ErrChanged
				}
			}
		} else {
			reply.Err = ErrConfigChanged
		}
	}
}

func (kv *ShardKV) commitKey(Ts TransactionStruct) {
	shard := key2shard(Ts.Key)
	if kv.ShardStatus[shard] == RECEIVING { // When shard group is receiving, put the decision into Decision Queue

		decision := Decision{
			TransactionID: Ts.TransactionNum,
			Key:           Ts.Key,
			Op:            COMMIT,
		}
		kv.DecisionQueue = append(kv.DecisionQueue, decision)

	} else if kv.ShardStatus[shard] == SERVE {

		if _, prs := kv.PreparedKeys[Ts.Key]; prs {
			delete(kv.PreparedKeys, Ts.Key)
			value, prs := kv.ShadowData[Ts.TransactionNum][shard][Ts.Key]
			if !prs {
				value = ""
			}

			kv.LocalData[shard][Ts.Key] = value

			delete(kv.ShadowData[Ts.TransactionNum][shard], Ts.Key)
			delete(kv.ShadowDataCompare[Ts.TransactionNum][shard], Ts.Key)
			if len(kv.ShadowData[Ts.TransactionNum][shard]) == 0 {
				delete(kv.ShadowData[Ts.TransactionNum], shard)
				delete(kv.ShadowDataCompare[Ts.TransactionNum], shard)
				if len(kv.ShadowData[Ts.TransactionNum]) == 0 {
					delete(kv.ShadowData, Ts.TransactionNum)
					delete(kv.ShadowDataCompare, Ts.TransactionNum)
				}
			}
		}

		if _, prs := kv.ShadowData[Ts.TransactionNum]; prs {
			if _, prs := kv.ShadowData[Ts.TransactionNum][shard]; prs {
				if _, prs := kv.ShadowData[Ts.TransactionNum][shard][Ts.Key]; prs {
					delete(kv.ShadowData[Ts.TransactionNum][shard], Ts.Key)
					delete(kv.ShadowDataCompare[Ts.TransactionNum][shard], Ts.Key)
					if len(kv.ShadowData[Ts.TransactionNum][shard]) == 0 {
						delete(kv.ShadowData[Ts.TransactionNum], shard)
						delete(kv.ShadowDataCompare[Ts.TransactionNum], shard)
						if len(kv.ShadowData[Ts.TransactionNum]) == 0 {
							delete(kv.ShadowData, Ts.TransactionNum)
							delete(kv.ShadowDataCompare, Ts.TransactionNum)
						}
					}
				}
			}
		}

		delete(kv.TransactionTimers, Ts.TransactionNum)
	}
}

func (kv *ShardKV) abortTransaction(Ts TransactionStruct) {
	shard := key2shard(Ts.Key)
	if kv.ShardStatus[shard] == RECEIVING {

		decision := Decision{
			TransactionID: Ts.TransactionNum,
			Key:           Ts.Key,
			Op:            ABORT,
		}
		kv.DecisionQueue = append(kv.DecisionQueue, decision)

	} else if kv.ShardStatus[shard] == SERVE {
		if _, prs := kv.PreparedKeys[Ts.Key]; prs {
			delete(kv.PreparedKeys, Ts.Key)

			delete(kv.ShadowData[Ts.TransactionNum][shard], Ts.Key)
			delete(kv.ShadowDataCompare[Ts.TransactionNum][shard], Ts.Key)
			if len(kv.ShadowData[Ts.TransactionNum][shard]) == 0 {
				delete(kv.ShadowData[Ts.TransactionNum], shard)
				delete(kv.ShadowDataCompare[Ts.TransactionNum], shard)
				if len(kv.ShadowData[Ts.TransactionNum]) == 0 {
					delete(kv.ShadowData, Ts.TransactionNum)
					delete(kv.ShadowDataCompare, Ts.TransactionNum)
				}
			}
		}

		if _, prs := kv.ShadowData[Ts.TransactionNum]; prs {
			if _, prs := kv.ShadowData[Ts.TransactionNum][shard]; prs {
				if _, prs := kv.ShadowData[Ts.TransactionNum][shard][Ts.Key]; prs {
					delete(kv.ShadowData[Ts.TransactionNum][shard], Ts.Key)
					delete(kv.ShadowDataCompare[Ts.TransactionNum][shard], Ts.Key)
					if len(kv.ShadowData[Ts.TransactionNum][shard]) == 0 {
						delete(kv.ShadowData[Ts.TransactionNum], shard)
						delete(kv.ShadowDataCompare[Ts.TransactionNum], shard)
						if len(kv.ShadowData[Ts.TransactionNum]) == 0 {
							delete(kv.ShadowData, Ts.TransactionNum)
							delete(kv.ShadowDataCompare, Ts.TransactionNum)
						}
					}
				}
			}
		}

		delete(kv.TransactionTimers, Ts.TransactionNum)
	}
}

func (kv *ShardKV) setUpShadow(Key string, TransactionNum int) {
	shard := key2shard(Key)

	if _, prs := kv.ShadowDataCompare[TransactionNum]; !prs {
		kv.ShadowDataCompare[TransactionNum] = make(map[int]map[string]string)
		kv.ShadowData[TransactionNum] = make(map[int]map[string]string)
		kv.TransactionTimers[TransactionNum] = time.NewTimer(TransactionTimeOut)
	}

	if _, prs := kv.ShadowDataCompare[TransactionNum][shard]; !prs {
		kv.ShadowDataCompare[TransactionNum][shard] = make(map[string]string)
		kv.ShadowData[TransactionNum][shard] = make(map[string]string)
		kv.TransactionTimers[TransactionNum].Reset(TransactionTimeOut)
	}

	if _, prs := kv.ShadowDataCompare[TransactionNum][shard][Key]; !prs {
		if _, prs := kv.LocalData[shard][Key]; prs {
			kv.ShadowDataCompare[TransactionNum][shard][Key] = kv.LocalData[shard][Key]
			kv.ShadowData[TransactionNum][shard][Key] = kv.LocalData[shard][Key]
			kv.TransactionTimers[TransactionNum].Reset(TransactionTimeOut)
		}
	}
}

func (kv *ShardKV) checkIsInTransaction(Key string) bool {
	shard := key2shard(Key)

	for tnum, v := range kv.ShadowDataCompare {
		if _, prs := v[shard]; !prs {
			continue
		}

		if _, prs := v[shard][Key]; prs {
			DPrintf("Key %v is hang on Transaction %d. \n", Key, tnum)
			return true
		}

	}

	return false
}

func (kv *ShardKV) runApplyOp() {
	for {

		if kv.Killed {
			return
		}

		am := <-kv.applyCh

		if am.CommandValid && am.Command != nil {
			operation := am.Command.(Op)
			reply := OpReply{}

			kv.mu.Lock()
			if operation.OpType == NEW_CONFIG {

				kv.applyNewConfig(operation.NewConfig)

			} else if operation.OpType == RECEIVED_SHARD {

				if operation.ReceivedShard.ConfigNum == kv.ApplyingConfigNum-1 {
					kv.applyReceivedData(operation.ReceivedShard)
					reply.Err = OK
				} else {
					DPrintf("ShardKV %d at %d does not apply the received data at shard %d. \n", kv.me, kv.gid, operation.ReceivedShard.Shard)
					if operation.ReceivedShard.ConfigNum != kv.ApplyingConfigNum-1 {
						DPrintf("ShardKV %d at %d received config %d, but applying config %d. \n", kv.me, kv.gid, operation.ReceivedShard.ConfigNum, kv.ApplyingConfigNum)
					}

					reply.Err = ErrWrongGroup
				}

			} else if operation.OpType == SENDING_SHARD {
				kv.applySendingData(operation.SendingShard)
				reply.Err = OK

			} else if operation.OpType == CLINET_OP {
				cOp := operation.ClientOp
				shard := key2shard(cOp.Key)

				if cOp.ConfigNum == kv.ApplyingConfigNum {
					if kv.ShardStatus[shard] == SERVE {

						lsn, prs := kv.LatestClientOpSeqNum[shard][cOp.ClientID]
						if cOp.TransactionNum > 0 {
							if cOp.OpName != GET && (!prs || (prs && lsn < cOp.ClientOpSeqNum)) {

								kv.setUpShadow(cOp.Key, cOp.TransactionNum)

								switch cOp.OpName {
								case PUT:
									kv.ShadowData[cOp.TransactionNum][shard][cOp.Key] = cOp.Value
								case APPEND:
									kv.ShadowData[cOp.TransactionNum][shard][cOp.Key] += cOp.Value
								}

								kv.LatestClientOpSeqNum[shard][cOp.ClientID] = cOp.ClientOpSeqNum
								DPrintf("ShardKV %d at %d applied the client requets %v at index %d. \n", kv.me, kv.gid, cOp.OpName, am.CommandIndex)
							}
							reply.Err = OK
						} else {
							if cOp.OpName != GET && (!prs || (prs && lsn < cOp.ClientOpSeqNum)) {
								if kv.checkIsInTransaction(cOp.Key) {
									reply.Err = ErrInTransaction
								} else {
									switch cOp.OpName {
									case PUT:
										kv.LocalData[shard][cOp.Key] = cOp.Value
									case APPEND:
										kv.LocalData[shard][cOp.Key] += cOp.Value
									}

									kv.LatestClientOpSeqNum[shard][cOp.ClientID] = cOp.ClientOpSeqNum
									DPrintf("ShardKV %d at %d applied the client requets %v at index %d. \n", kv.me, kv.gid, cOp.OpName, am.CommandIndex)
									reply.Err = OK
								}
							} else {
								reply.Err = OK
							}
						}

					} else if kv.ShardStatus[shard] == RECEIVING {
						reply.Err = ErrWaiting
					} else {
						reply.Err = ErrWrongGroup
					}
				} else {
					reply.Err = ErrWrongGroup
				}
			} else if operation.OpType == TRANSACTION {
				Ts := operation.Transaction
				switch Ts.OpName {
				case PREPARE:
					prepareReply := &PrepareReply{}
					kv.setUpPrepare(Ts, prepareReply)
					reply.Err = prepareReply.Err
					// kv.createSnapshot()
					// go kv.Rf.LocalCompactSnapshot(am.CommandIndex)
				case COMMIT:
					// kv.createSnapshot()
					// go kv.Rf.LocalCompactSnapshot(am.CommandIndex)

					kv.commitKey(Ts)

					// kv.createSnapshot()
					// go kv.Rf.LocalCompactSnapshot(am.CommandIndex)
					reply.Err = OK
				case ABORT:
					kv.abortTransaction(Ts)
					reply.Err = OK
					// kv.createSnapshot()
					// go kv.Rf.LocalCompactSnapshot(am.CommandIndex)
				}
			}

			kv.mu.Unlock()

			kv.mu.Lock()
			if cChan, prs := kv.LogIndexCallbackChanMap[am.CommandIndex]; prs && cChan != nil {
				kv.mu.Unlock()
				cChan <- reply
				kv.mu.Lock()
				delete(kv.LogIndexCallbackChanMap, am.CommandIndex)
				DPrintf("ShardKV %d at %d send back to chan at index %d. \n", kv.me, kv.gid, am.CommandIndex)
			}
			kv.mu.Unlock()

			if kv.maxraftstate != -1 && kv.Rf.GetRaftStateSize() >= kv.maxraftstate {
				kv.mu.Lock()
				DPrintf("ShardKV %d at %d Log size reach the max raft, will create the snapshot. \n", kv.me, kv.gid)
				kv.createSnapshot()
				DPrintf("ShardKV %d at %d Log size reach the max raft, create the snapshot. \n", kv.me, kv.gid)
				kv.mu.Unlock()
				go kv.Rf.LocalCompactSnapshot(am.CommandIndex)
			}

		} else if !am.CommandValid && am.IsSnapshot {
			DPrintf("ShardKV %d at %d received apply snapshot. \n", kv.me, kv.gid)
			kv.mu.Lock()
			kv.readSnapshot(am.SnapshotData)
			DPrintf("ShardKV %d at %d read the snapshot. \n", kv.me, kv.gid)
			kv.mu.Unlock()
		}
	}
}

// The Logic of this function comes form internet resources: https://github.com/wqlin/mit-6.824-2018/blob/solution/src/shardkv/server.go
func (kv *ShardKV) runCheckShardReceivingStatus() {

	if _, isLeader := kv.Rf.GetState(); isLeader {

		kv.mu.Lock()

		total := 0
		var mChan chan struct{}
		if len(kv.ReceivingShards) > 0 {
			mChan = make(chan struct{})
			for shard, configNum := range kv.ReceivingShards {
				total++

				config := kv.shardMaster.Query(configNum)

				go kv.AddNewShard(shard, config, mChan)
			}
		}

		if total > 0 {
			DPrintf("ShardKV %d at %d is receiving %d shards at config num %d. \n", kv.me, kv.gid, total, kv.ApplyingConfigNum)
		}

		kv.mu.Unlock()

		for total > 0 {
			<-mChan
			total--
		}

	}
}

func (kv *ShardKV) runCheckTransactionTimeOut() {
	for {
		<-kv.TransactionTimeoutTimer.C

		kv.mu.Lock()
		tnums := make([]int, 0)
		for tnum, timer := range kv.TransactionTimers {
			to := false
			select {
			case <-timer.C:
				to = true
			default:
				break
			}

			if to {
				tnums = append(tnums, tnum)
			}
		}

		if len(tnums) > 0 {
			for tnum := range tnums {
				for _, data := range kv.ShadowDataCompare[tnum] {
					for key := range data {
						ts := TransactionStruct{TransactionNum: tnum, Key: key}
						kv.abortTransaction(ts)
					}
				}
			}
		}

		kv.mu.Unlock()

		kv.TransactionTimeoutTimer.Reset(200 * time.Millisecond)
	}
}

// // Deleteing shard not really work
// func (kv *ShardKV) runCheckShardSendingStatus() {
// 	for {
// 		if _, isLeader := kv.rf.GetState(); isLeader {

// 			kv.mu.Lock()

// 			for k := range kv.ReceivedShards {
// 				if kv.ShardStatus[k] == SERVE {
// 					kv.mu.Unlock()
// 					kv.RemoveAShard(k)
// 					kv.mu.Lock()
// 				}
// 			}

// 			kv.mu.Unlock()
// 		}

// 		time.Sleep(raft.HEART_BEAT_INTERVAL * time.Millisecond)
// 	}
// }

func (kv *ShardKV) runCheckNewConfig() {

	if _, isLeader := kv.Rf.GetState(); isLeader {

		kv.mu.Lock()
		latestConfig := kv.shardMaster.Query(-1)

		if latestConfig.Num > kv.ApplyingConfigNum {

			if len(kv.ReceivingShards) == 0 {
				newConfig := kv.shardMaster.Query(kv.ApplyingConfigNum + 1)
				go kv.registerNewConfig(newConfig)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) runChecking() {
	for {
		select {
		case <-kv.configTimer.C:
			kv.runCheckNewConfig()
			kv.configTimer.Reset(80 * time.Millisecond)
		case <-kv.receivingTimer.C:
			kv.runCheckShardReceivingStatus()
			kv.receivingTimer.Reset(50 * time.Millisecond)
		case <-kv.KillChan:
			return
		}
	}
}

func (kv *ShardKV) PrintShardStatus() {
	fmt.Printf("ShardKV %d at %d 's shards at applying config num %d: \n", kv.me, kv.gid, kv.ApplyingConfigNum)
	for i := range kv.ShardStatus {
		fmt.Printf(" %d ", i)
	}

	fmt.Printf("\n")
	fmt.Printf("ShardKV %d at %d 's shards' mode: \n", kv.me, kv.gid)
	for i := range kv.ShardStatus {
		fmt.Printf(" %d ", kv.ShardStatus[i])
	}
	fmt.Printf("\n")
}

// func (kv *ShardKV) runPrintShardStatus() {
// 	for {

// 		if _, isLeader := kv.rf.GetState(); isLeader {

// 			kv.mu.Lock()
// 			DPrintf("ShardKV %d at %d 's shards: \n", kv.me, kv.gid)
// 			for i := range kv.ShardStatus {
// 				fmt.Printf(" %d ", i)
// 			}

// 			fmt.Printf("\n")
// 			DPrintf("ShardKV %d at %d 's shards' mode: \n", kv.me, kv.gid)
// 			for i := range kv.ShardStatus {
// 				fmt.Printf(" %d ", kv.ShardStatus[i])
// 			}
// 			fmt.Printf("\n")

// 			// go printConfig(kv.shardMaster.Query(kv.ApplyingConfigNum))

// 			kv.mu.Unlock()

// 		}

// 		time.Sleep(300 * time.Millisecond)
// 	}
// }

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {

	close(kv.KillChan)
	kv.Killed = true

	go kv.Rf.Kill()
	DPrintf("ShardKV %d at %d ----- Killed! ------ \n", kv.me, kv.gid)
	fmt.Printf("")
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(DeleteShardDataArgs{})
	labgob.Register(DeleteShardDataReply{})
	labgob.Register(RequestShardDataArgs{})
	labgob.Register(RequestShardDataReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.LocalData = make(map[int]map[string]string)
	kv.LatestClientOpSeqNum = make(map[int]map[int64]int)
	kv.LogIndexCallbackChanMap = make(map[int]chan OpReply)
	kv.PendingData = make(map[int]map[int]map[string]string)
	kv.PendingOpSeqNum = make(map[int]map[int]map[int64]int)
	kv.ReceivingShards = make(map[int]int)
	kv.Killed = false
	kv.KillChan = make(chan struct{})

	kv.ApplyingConfigNum = 0

	for i := range kv.ShardStatus {
		kv.ShardStatus[i] = UNSERVE
	}

	kv.ShadowData = make(map[int]map[int]map[string]string)
	kv.ShadowDataCompare = make(map[int]map[int]map[string]string)
	kv.PreparedKeys = make(map[string]int)

	kv.PendingShadowDataCompare = make(map[int]map[int]map[int]map[string]string)
	kv.PendingShadowData = make(map[int]map[int]map[int]map[string]string)
	kv.PendingPreparedKeys = make(map[int]map[string]int)
	kv.DecisionQueue = make([]Decision, 0)
	kv.shardMaster = shardmaster.MakeClerk(masters)
	kv.TransactionTimers = make(map[int]*time.Timer)

	kv.applyCh = make(chan raft.ApplyMsg)

	if snapshotData := persister.ReadSnapshot(); kv.maxraftstate != -1 && snapshotData != nil && len(snapshotData) > 0 {
		kv.readSnapshot(snapshotData)
	}

	kv.Rf = raft.Make(servers, me, persister, kv.applyCh)

	DPrintf("ShardKV %d at %d ***** Started! *****. Config at %d. \n", kv.me, kv.gid, kv.ApplyingConfigNum)

	kv.configTimer = time.NewTimer(80 * time.Millisecond)
	kv.receivingTimer = time.NewTimer(raft.HEART_BEAT_INTERVAL * time.Millisecond)
	kv.TransactionTimeoutTimer = time.NewTimer(200 * time.Millisecond)

	go kv.runCheckTransactionTimeOut()
	go kv.runChecking()
	go kv.runApplyOp()

	return kv
}

func (kv *ShardKV) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&kv.ReceivingShards)
	d.Decode(&kv.LocalData)
	d.Decode(&kv.LatestClientOpSeqNum)
	d.Decode(&kv.ApplyingConfigNum)
	d.Decode(&kv.ShardStatus)
	d.Decode(&kv.PendingData)
	d.Decode(&kv.PendingOpSeqNum)
	d.Decode(&kv.ShadowData)
	d.Decode(&kv.ShadowDataCompare)
	d.Decode(&kv.PreparedKeys)
	d.Decode(&kv.PendingShadowData)
	d.Decode(&kv.PendingShadowDataCompare)
	d.Decode(&kv.PendingPreparedKeys)
	d.Decode(&kv.DecisionQueue)
}

func (kv *ShardKV) createSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.ReceivingShards)
	e.Encode(kv.LocalData)
	e.Encode(kv.LatestClientOpSeqNum)
	e.Encode(kv.ApplyingConfigNum)
	e.Encode(kv.ShardStatus)
	e.Encode(kv.PendingData)
	e.Encode(kv.PendingOpSeqNum)
	e.Encode(kv.ShadowData)
	e.Encode(kv.ShadowDataCompare)
	e.Encode(kv.PreparedKeys)
	e.Encode(kv.PendingShadowData)
	e.Encode(kv.PendingShadowDataCompare)
	e.Encode(kv.PendingPreparedKeys)
	e.Encode(kv.DecisionQueue)

	kv.Rf.SaveSnapshot(w.Bytes())
}

func printConfig(config shardmaster.Config) {
	fmt.Printf("Print Config Num %d, Shards: \n", config.Num)
	for i := range config.Shards {
		fmt.Printf("%d  ", i)
	}
	fmt.Printf("\nGID: ")
	for i := range config.Shards {
		fmt.Printf("%d  ", config.Shards[i])
	}
	fmt.Printf("\n")
}
