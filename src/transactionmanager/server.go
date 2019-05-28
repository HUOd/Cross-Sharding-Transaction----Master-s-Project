package transactionmanager

import (
	"cst/src/labgob"
	"cst/src/labrpc"
	"cst/src/raft"
	"cst/src/shardkv"
	"log"
	"strconv"
	"sync"
	"time"
)

const ( // Op type
	CLIENT_OP = "ClientOp"
	TS_OP     = "TsOp"
	BLOCK_OP  = "BlockOp"
)

const ( // Client Op
	GET    = "GET"
	PUT    = "Put"
	APPEND = "Append"
)

const ( // Transaction Op
	BEGIN   = "Begin"
	START   = "Start"
	PREPARE = "Prepare"
	COMMIT  = "Commit"
	ABORT   = "Abort"
	UPDATE  = "Update"
)

const ( // Transaction Update Op
	ADD    = "Add"
	REMOVE = "Remove"
)

const ( // Transaction State
	ActiveState  = "Active"
	StartState   = "Start"
	PrepareState = "Prepare"
	CommitState  = "Commit"
	AbortState   = "Abort"
)

type TsState string

// Ts is the basic Transaction Record stored in Transaction Manager
type Ts struct {
	Num      int
	State    TsState
	Ops      []ClientOpStruct
	KeyToGid map[string]int
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type TransactionManager struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	Killed bool

	shardKVClientQ     *kvClientQ
	ClientQ            *clientQ
	ClientLastOpSeqNum map[int64]int
	CallbackChanMap    map[int]chan OpReply
	Transactions       []*Ts
	// When Rpc call failed, TM should continuely send the request to avoid blocking on shard
	UndeliveredAbortKeys  map[int]map[string]bool
	UndeliveredCommitKeys map[int]map[string]bool
	checkTimer            *time.Timer
}

type Op struct {
	OpType   string
	ClientOp ClientOpStruct
	TsOp     TransactionOpStruct
	BlockOp  BlockingOpStruct
}

type ClientOpStruct struct {
	OpName         string
	ClientID       int64
	ClientOpSeqNum int
	TransactionID  int
	Key            string
	Value          string
}

type TransactionOpStruct struct {
	OpName         string
	ClientID       int64
	ClientOpSeqNum int
	TransactionID  int
	Key            string
	Gid            int
}

type BlockingOpStruct struct {
	OpName         string
	BlockingType   string
	ClientID       int64
	ClientOpSeqNum int
	TransactionID  int
	Key            string
}

type OpReply struct {
	Err   Err
	Value string
}

type GeneralReply struct {
	Err   Err
	Index int
}

func (tm *TransactionManager) Get(args *GetArgs, reply *GetReply) {
	DPrintf("TransactionManager %d received Get Request for Transaction %d from Client %d. \n", tm.me, args.TransactionID, args.ClientID)

	thisOperation := Op{
		OpType: CLIENT_OP,
		ClientOp: ClientOpStruct{
			OpName:         GET,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			Key:            args.Key,
			TransactionID:  args.TransactionID,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Get Request for Transaction %d at index %d. \n", tm.me, args.TransactionID, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						// OpIndex, _ := strconv.Atoi(Or.Value)
						tm.mu.Lock()
						ts := tm.Transactions[args.TransactionID]
						tm.mu.Unlock()

						skv := tm.shardKVClientQ.deQkvClient()
						value, err, gid := skv.TGet(args.Key, args.TransactionID)
						tm.shardKVClientQ.enQkvClient(skv)
						if err != OK {
							DPrintf("TransactionManager %d received reply of Get with Err %v, will abort Transaction %d. \n", tm.me, args.ClientID, err, args.TransactionID)
							client := tm.ClientQ.deQClient()
							reply.Err = client.Update(args.TransactionID, args.Key, gid)
							tm.ClientQ.enQClient(client)

							client = tm.ClientQ.deQClient()
							client.Abort(args.TransactionID)
							tm.ClientQ.enQClient(client)
							reply.Err = Err(err)
							return
						}

						if preGid, prs := ts.KeyToGid[args.Key]; prs {
							if preGid != gid {
								DPrintf("TransactionManager %d received reply of Get, gid not match, prevGid %d curGid %d, will abort Transaction %d. \n", tm.me, args.ClientID, preGid, gid, args.TransactionID)
								client := tm.ClientQ.deQClient()
								reply.Err = client.Update(args.TransactionID, args.Key, gid)
								tm.ClientQ.enQClient(client)

								client = tm.ClientQ.deQClient()
								client.Abort(args.TransactionID)
								tm.ClientQ.enQClient(client)
								reply.Err = ErrConfigChanged
								return
							}
							reply.Err = OK
							reply.Value = value
						} else {
							client := tm.ClientQ.deQClient()
							reply.Err = client.Update(args.TransactionID, args.Key, gid)
							tm.ClientQ.enQClient(client)
							if reply.Err == OK {
								reply.Value = value
							}
							return
						}
					} else {
						DPrintf("TransactionManager %d get Err %v of Get Requet of index %d. \n", tm.me, Or.Err, newIndex)
						reply.Err = Or.Err
					}
				} else {
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("TransactionManager %d received %v Request for Transaction %d from Client %d. \n", tm.me, args.Op, args.TransactionID, args.ClientID)

	thisOperation := Op{
		OpType: CLIENT_OP,
		ClientOp: ClientOpStruct{
			OpName:         args.Op,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			Key:            args.Key,
			Value:          args.Value,
			TransactionID:  args.TransactionID,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started %v Request for Transaction %d from at index %d. \n", tm.me, args.Op, args.TransactionID, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						tm.mu.Lock()
						ts := tm.Transactions[args.TransactionID]
						tm.mu.Unlock()

						var err shardkv.Err
						var gid int
						if args.Op == PUT {
							skv := tm.shardKVClientQ.deQkvClient()
							err, gid = skv.TPut(args.Key, args.Value, args.TransactionID)
							tm.shardKVClientQ.enQkvClient(skv)
						} else {
							skv := tm.shardKVClientQ.deQkvClient()
							err, gid = skv.TAppend(args.Key, args.Value, args.TransactionID)
							tm.shardKVClientQ.enQkvClient(skv)
						}

						if err != OK {
							DPrintf("TransactionManager %d received reply of %v with Err %v, will abort Transaction %d. \n", tm.me, args.ClientID, args.Op, err, args.TransactionID)
							client := tm.ClientQ.deQClient()
							reply.Err = client.Update(args.TransactionID, args.Key, gid)
							tm.ClientQ.enQClient(client)

							client = tm.ClientQ.deQClient()
							client.Abort(args.TransactionID)
							tm.ClientQ.enQClient(client)
							reply.Err = Err(err)
							return
						}

						if preGid, prs := ts.KeyToGid[args.Key]; prs {
							if preGid != gid {
								DPrintf("TransactionManager %d received reply of %v, gid not match, prevGid %d curGid %d, will abort Transaction %d. \n", tm.me, args.ClientID, args.Op, preGid, gid, args.TransactionID)

								client := tm.ClientQ.deQClient()
								reply.Err = client.Update(args.TransactionID, args.Key, gid)
								tm.ClientQ.enQClient(client)

								client = tm.ClientQ.deQClient()
								client.Abort(args.TransactionID)
								tm.ClientQ.enQClient(client)
								reply.Err = ErrConfigChanged
								return
							}
							reply.Err = OK
						} else {
							client := tm.ClientQ.deQClient()
							reply.Err = client.Update(args.TransactionID, args.Key, gid)
							tm.ClientQ.enQClient(client)
							return
						}

					} else {
						DPrintf("TransactionManager %d get Err %v of %v Requet of index %d. \n", tm.me, Or.Err, args.Op, newIndex)
						reply.Err = Or.Err
					}
				} else {
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) Begin(args *BeginArgs, reply *BeginReply) {
	DPrintf("TransactionManager %d received Begin Request from Client %d. \n", tm.me, args.ClientID)
	thisOperation := Op{
		OpType: TS_OP,
		TsOp: TransactionOpStruct{
			OpName:         BEGIN,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Begin Request at index %d. \n", tm.me, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						reply.Err = OK
						reply.TransactionID, _ = strconv.Atoi(Or.Value)
						DPrintf("TransactionManager %d create a new Transaction %d. \n", tm.me, reply.TransactionID)
					} else {
						reply.Err = Or.Err
						DPrintf("TransactionManager %d received reply of Begin with Err %v. \n", tm.me, reply.Err)
					}
				} else {
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) Commit(args *CommitArgs, reply *CommitReply) {
	DPrintf("TransactionManager %d received Commit Request for Transaction %d from Client %d. \n", tm.me, args.TransactionID, args.ClientID)

	tm.mu.Lock()
	if tm.Transactions[args.TransactionID].State != ActiveState {
		tm.mu.Unlock()
		return
	}
	tm.mu.Unlock()

	thisOperation := Op{
		OpType: TS_OP,
		TsOp: TransactionOpStruct{
			OpName:         START,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			TransactionID:  args.TransactionID,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Commit Request for Transaction %d at index %d. \n", tm.me, args.TransactionID, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					tm.mu.Lock()
					ts := tm.Transactions[args.TransactionID]
					tm.mu.Unlock()

					keys := make([]string, 0)
					for key := range ts.KeyToGid {
						keys = append(keys, key)
					}

					total := len(keys)
					votes := 0
					voteChan := make(chan bool)
					for _, key := range keys {
						go func(key string) {
							skv := tm.shardKVClientQ.deQkvClient()
							DPrintf("TransactionManager %d sent Prepare requests for Transaction %d. \n", tm.me, args.TransactionID)
							res, err := skv.Prepare(key, args.TransactionID)
							tm.shardKVClientQ.enQkvClient(skv)
							if err == OK {
								if res {
									voteChan <- true
								} else {
									voteChan <- false
								}
							} else {
								DPrintf("TransactionManager %d received reply of Prepare with Err %v, will abort Transaction %d. \n", tm.me, err, args.TransactionID)
								voteChan <- false
							}
						}(key)
					}

					for i := 0; i < total; i++ {
						if <-voteChan {
							votes++
						}
					}

					if votes == total {
						DPrintf("TransactionManager %d received %d votes, start Commit for Transaction %d. \n", tm.me, votes, args.TransactionID)
						client := tm.ClientQ.deQClient()
						client.UpdateState(args.TransactionID, PrepareState)
						tm.ClientQ.enQClient(client)

						rChan := make(chan struct{})
						for _, key := range keys {

							go func(key string) {
								skv := tm.shardKVClientQ.deQkvClient()
								res, _ := skv.Commit(key, args.TransactionID)
								tm.shardKVClientQ.enQkvClient(skv)

								if !res {
									client := tm.ClientQ.deQClient()
									client.AddBlockKey(args.TransactionID, key, COMMIT)
									tm.ClientQ.enQClient(client)
								}
								rChan <- struct{}{}
							}(key)
						}

						for total > 0 {
							<-rChan
							total--
						}

						client = tm.ClientQ.deQClient()
						client.UpdateState(args.TransactionID, CommitState)
						tm.ClientQ.enQClient(client)
						reply.Err = OK
						DPrintf("TransactionManager %d Commit Complete on Transaction %d. \n", tm.me, args.TransactionID)
					} else {
						DPrintf("TransactionManager %d received reply of Prepare vote NO, will abort Transaction %d. \n", tm.me, args.TransactionID)
						client := tm.ClientQ.deQClient()
						client.Abort(args.TransactionID)
						tm.ClientQ.enQClient(client)

						reply.Err = ErrAbort
						break
					}

				} else {
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) Abort(args *AbortArgs, reply *AbortReply) {
	DPrintf("TransactionManager %d received Abort Request for Transaction %d from Client %d. \n", tm.me, args.TransactionID, args.ClientID)

	tm.mu.Lock()
	if tm.Transactions[args.TransactionID].State == AbortState {
		tm.mu.Unlock()
		return
	}
	tm.mu.Unlock()

	thisOperation := Op{
		OpType: TS_OP,
		TsOp: TransactionOpStruct{
			OpName:         ABORT,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			TransactionID:  args.TransactionID,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Abort Request for Transaction %d at index %d. \n", tm.me, args.TransactionID, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					tm.mu.Lock()
					ts := tm.Transactions[args.TransactionID]
					tm.mu.Unlock()

					DPrintf("TransactionManager %d start Abort the Transaction %d. \n", tm.me, args.TransactionID)
					keys := make([]string, 0)
					for key := range ts.KeyToGid {
						keys = append(keys, key)
					}

					rChan := make(chan struct{})
					total := len(keys)
					for _, key := range keys {
						go func(key string) {
							skv := tm.shardKVClientQ.deQkvClient()
							DPrintf("TransactionManager %d send Abort the Transaction %d to kvclient %d. \n", tm.me, args.TransactionID, skv.ClientID)
							res, _ := skv.Abort(key, args.TransactionID)
							tm.shardKVClientQ.enQkvClient(skv)

							if !res {
								client := tm.ClientQ.deQClient()
								client.AddBlockKey(args.TransactionID, key, ABORT)
								tm.ClientQ.enQClient(client)
							}
							rChan <- struct{}{}
						}(key)
					}

					for total > 0 {
						<-rChan
						total--
					}

					reply.Err = OK
					DPrintf("TransactionManager %d Abort the Transaction %d. \n", tm.me, args.TransactionID)
				} else {
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		DPrintf("TransactionManager %d is no longer the leader. \n", tm.me)
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) Update(args *UpdateArgs, reply *UpdateReply) {
	thisOperation := Op{
		OpType: TS_OP,
		TsOp: TransactionOpStruct{
			OpName:         UPDATE,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			Key:            args.Key,
			Gid:            args.Gid,
			TransactionID:  args.TransactionID,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Update Request at index %d. \n", tm.me, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						reply.Err = OK
					} else {
						reply.Err = Or.Err
					}
				} else {
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) UpdateState(args *UpdateStateArgs, reply *UpdateStateReply) {
	var OpName string
	if args.State == PrepareState {
		OpName = PREPARE
	} else {
		OpName = COMMIT
	}

	thisOperation := Op{
		OpType: TS_OP,
		TsOp: TransactionOpStruct{
			OpName:         OpName,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			TransactionID:  args.TransactionID,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started UpdateState Request at index %d. \n", tm.me, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						reply.Err = OK
					} else {
						reply.Err = Or.Err
					}
				} else {
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) AddBlockingKey(args *AddBlockingKeyArgs, reply *AddBlockingKeyReply) {
	thisOperation := Op{
		OpType: BLOCK_OP,
		BlockOp: BlockingOpStruct{
			OpName:         ADD,
			BlockingType:   args.BlockingType,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			Key:            args.Key,
			TransactionID:  args.TransactionNum,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Add Blocking Request at index %d. \n", tm.me, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						reply.Err = OK
					} else {
						reply.Err = Or.Err
					}
				} else {
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) RemoveBlockingKey(args *RemoveBlockingKeyArgs, reply *RemoveBlockingKeyReply) {
	thisOperation := Op{
		OpType: BLOCK_OP,
		BlockOp: BlockingOpStruct{
			OpName:         REMOVE,
			BlockingType:   args.BlockingType,
			ClientID:       args.ClientID,
			ClientOpSeqNum: args.ClientLastOpSeqNum,
			Key:            args.Key,
			TransactionID:  args.TransactionNum,
		},
	}

	newIndex, thisTerm, isLeader := tm.rf.Start(thisOperation)

	if isLeader {
		tm.mu.Lock()
		mChan := make(chan OpReply)
		tm.CallbackChanMap[newIndex] = mChan
		DPrintf("TransactionManager %d started Remove Blocking Request at index %d. \n", tm.me, newIndex)
		tm.mu.Unlock()

		for {
			select {
			case Or := <-mChan:
				if curTerm, isLeader := tm.rf.GetState(); isLeader && thisTerm == curTerm {
					if Or.Err == OK {
						reply.Err = OK
					} else {
						reply.Err = Or.Err
					}
				} else {
					reply.WrongLeader = true
				}
				return
			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader := tm.rf.GetState(); !isLeader {
					tm.mu.Lock()
					delete(tm.CallbackChanMap, newIndex)
					tm.mu.Unlock()
					reply.WrongLeader = true
					return
				}
			}
		}
	} else {
		reply.WrongLeader = true
	}
}

func (tm *TransactionManager) AddBlockingKeyApply(BlockOp BlockingOpStruct) {
	if BlockOp.BlockingType == COMMIT {
		if _, prs := tm.UndeliveredCommitKeys[BlockOp.TransactionID]; !prs {
			tm.UndeliveredCommitKeys[BlockOp.TransactionID] = make(map[string]bool)
		}

		tm.UndeliveredCommitKeys[BlockOp.TransactionID][BlockOp.Key] = true
	} else if BlockOp.BlockingType == ABORT {
		if _, prs := tm.UndeliveredAbortKeys[BlockOp.TransactionID]; !prs {
			tm.UndeliveredAbortKeys[BlockOp.TransactionID] = make(map[string]bool)
		}

		tm.UndeliveredAbortKeys[BlockOp.TransactionID][BlockOp.Key] = true
	}
}

func (tm *TransactionManager) RemoveBlockingKeyApply(BlockOp BlockingOpStruct) {
	if BlockOp.BlockingType == COMMIT {
		if _, prs := tm.UndeliveredCommitKeys[BlockOp.TransactionID]; !prs {
			return
		}

		delete(tm.UndeliveredCommitKeys[BlockOp.TransactionID], BlockOp.Key)
		if len(tm.UndeliveredCommitKeys[BlockOp.TransactionID]) == 0 {
			delete(tm.UndeliveredCommitKeys, BlockOp.TransactionID)
		}
	} else if BlockOp.BlockingType == ABORT {
		if _, prs := tm.UndeliveredAbortKeys[BlockOp.TransactionID]; !prs {
			return
		}

		delete(tm.UndeliveredAbortKeys[BlockOp.TransactionID], BlockOp.Key)
		if len(tm.UndeliveredAbortKeys[BlockOp.TransactionID]) == 0 {
			delete(tm.UndeliveredAbortKeys, BlockOp.TransactionID)
		}
	}
}

func (tm *TransactionManager) AddClientOp(ClientOp ClientOpStruct, reply *GeneralReply) {
	ts := tm.Transactions[ClientOp.TransactionID]

	if ts.State != ActiveState {
		reply.Err = ErrNotActive
		return
	}

	index := len(ts.Ops)
	ts.Ops = append(ts.Ops, ClientOp)
	reply.Index = index
	reply.Err = OK
}

func (tm *TransactionManager) ChangeTransactionState(TID int, State TsState, reply *GeneralReply) {
	ts := tm.Transactions[TID]

	switch State {
	case StartState:
		if ts.State != ActiveState {
			reply.Err = ErrDiplicateState
		} else {
			ts.State = State
			reply.Err = OK
		}
	case PrepareState:
		if ts.State == StartState || ts.State == ActiveState {
			ts.State = State
			reply.Err = OK
		} else {
			reply.Err = ErrDiplicateState
		}
	case CommitState:
		if ts.State == PrepareState {
			ts.State = State
			reply.Err = OK
		} else {
			reply.Err = ErrDiplicateState
		}
	case AbortState:
		if ts.State != AbortState {
			ts.State = State
			reply.Err = OK
		} else {
			reply.Err = ErrDiplicateState
		}
	}
}

func (tm *TransactionManager) UpdateGid(TID int, Key string, Gid int, reply *GeneralReply) {
	ts := tm.Transactions[TID]

	if ts.State != ActiveState {
		reply.Err = ErrNotActive
	} else {
		ts.KeyToGid[Key] = Gid
		reply.Err = OK
	}
}

func (tm *TransactionManager) AddNewTransaction() int {
	newTs := &Ts{}
	index := len(tm.Transactions)
	newTs.Num = index
	newTs.State = ActiveState
	newTs.Ops = make([]ClientOpStruct, 1)
	newTs.KeyToGid = make(map[string]int)
	tm.Transactions = append(tm.Transactions, newTs)
	return index
}

func (tm *TransactionManager) runApplyOp() {
	for {
		am := <-tm.applyCh

		if am.CommandValid && am.Command != nil {
			operation := am.Command.(Op)
			reply := OpReply{}
			if operation.OpType == CLIENT_OP {
				ClientOp := operation.ClientOp
				tm.mu.Lock()

				lsn, prs := tm.ClientLastOpSeqNum[ClientOp.ClientID]

				if !prs || (prs && lsn < ClientOp.ClientOpSeqNum) {
					greply := &GeneralReply{}
					tm.AddClientOp(ClientOp, greply)
					if greply.Err == OK {
						reply.Err = OK
						reply.Value = strconv.Itoa(greply.Index)
					} else {
						reply.Err = greply.Err
					}

					tm.ClientLastOpSeqNum[ClientOp.ClientID] = ClientOp.ClientOpSeqNum
				}

				tm.mu.Unlock()
			} else if operation.OpType == TS_OP {
				TsOp := operation.TsOp
				tm.mu.Lock()
				lsn, prs := tm.ClientLastOpSeqNum[TsOp.ClientID]

				if !prs || (prs && lsn < TsOp.ClientOpSeqNum) {
					switch TsOp.OpName {
					case BEGIN:
						id := tm.AddNewTransaction()
						reply.Value = strconv.Itoa(id)
						reply.Err = OK
					case PREPARE:
						var greply GeneralReply
						tm.ChangeTransactionState(TsOp.TransactionID, PrepareState, &greply)
						reply.Err = greply.Err
					case COMMIT:
						var greply GeneralReply
						tm.ChangeTransactionState(TsOp.TransactionID, CommitState, &greply)
						reply.Err = greply.Err
					case START:
						var greply GeneralReply
						tm.ChangeTransactionState(TsOp.TransactionID, StartState, &greply)
						reply.Err = greply.Err
					case ABORT:
						var greply GeneralReply
						tm.ChangeTransactionState(TsOp.TransactionID, AbortState, &greply)
						reply.Err = greply.Err
					case UPDATE:
						var greply GeneralReply
						tm.UpdateGid(TsOp.TransactionID, TsOp.Key, TsOp.Gid, &greply)
						reply.Err = greply.Err
					}

					tm.ClientLastOpSeqNum[TsOp.ClientID] = TsOp.ClientOpSeqNum
				} else {
					reply.Err = OK
				}
				tm.mu.Unlock()
			} else if operation.OpType == BLOCK_OP {
				BlockOp := operation.BlockOp
				tm.mu.Lock()
				lsn, prs := tm.ClientLastOpSeqNum[BlockOp.ClientID]

				if !prs || (prs && lsn < BlockOp.ClientOpSeqNum) {
					switch BlockOp.OpName {
					case ADD:
						tm.AddBlockingKeyApply(BlockOp)
					case REMOVE:
						tm.RemoveBlockingKeyApply(BlockOp)
					}

					tm.ClientLastOpSeqNum[BlockOp.ClientID] = BlockOp.ClientOpSeqNum
				}
				reply.Err = OK

				tm.mu.Unlock()

			}

			tm.mu.Lock()
			if cChan, prs := tm.CallbackChanMap[am.CommandIndex]; prs && cChan != nil {
				tm.mu.Unlock()
				cChan <- reply
				tm.mu.Lock()
				delete(tm.CallbackChanMap, am.CommandIndex)
				DPrintf("TransactionManager %d send back to chan at index %d. \n", tm.me, am.CommandIndex)
			}
			tm.mu.Unlock()
		}
	}
}

func (tm *TransactionManager) runCheckBlock() {
	for {
		<-tm.checkTimer.C

		if _, isLeader := tm.rf.GetState(); isLeader {
			tm.mu.Lock()
			for tnum, keys := range tm.UndeliveredAbortKeys {
				for key := range keys {
					tm.mu.Unlock()
					skv := tm.shardKVClientQ.deQkvClient()
					DPrintf("TransactionManager %d send Abort the Transaction %d to kvclient %d. \n", tm.me, tnum, skv.ClientID)
					res, _ := skv.Abort(key, tnum)
					tm.shardKVClientQ.enQkvClient(skv)

					if res {
						client := tm.ClientQ.deQClient()
						client.RemoveBlockKey(tnum, key, ABORT)
						tm.ClientQ.enQClient(client)
					}

					tm.mu.Lock()
				}
			}
			tm.mu.Unlock()

			tm.mu.Lock()
			for tnum, keys := range tm.UndeliveredCommitKeys {
				for key := range keys {
					tm.mu.Unlock()
					skv := tm.shardKVClientQ.deQkvClient()
					DPrintf("TransactionManager %d send Abort the Transaction %d to kvclient %d. \n", tm.me, tnum, skv.ClientID)
					res, _ := skv.Commit(key, tnum)
					tm.shardKVClientQ.enQkvClient(skv)

					if res {
						client := tm.ClientQ.deQClient()
						client.RemoveBlockKey(tnum, key, COMMIT)
						tm.ClientQ.enQClient(client)
					}

					tm.mu.Lock()
				}
			}
			tm.mu.Unlock()
		}

		tm.checkTimer.Reset(200 * time.Millisecond)
	}
}

func (tm *TransactionManager) Kill() {
	tm.Killed = true
	tm.rf.Kill()
}

func (tm *TransactionManager) Raft() *raft.Raft {
	return tm.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, shardKvClients []*shardkv.Clerk) *TransactionManager {
	tm := new(TransactionManager)
	tm.me = me

	labgob.Register(Op{})
	tm.applyCh = make(chan raft.ApplyMsg)
	tm.CallbackChanMap = make(map[int]chan OpReply)
	tm.ClientLastOpSeqNum = make(map[int64]int)
	tm.Transactions = make([]*Ts, 1)
	// tm.masters = masters
	clients := make([]*Clerk, 20)
	for i := 0; i < 20; i++ {
		clients[i] = MakeClerk(servers)
	}
	tm.ClientQ = newClientQ(clients)
	tm.shardKVClientQ = newkvClientQ(shardKvClients)
	tm.UndeliveredAbortKeys = make(map[int]map[string]bool)
	tm.UndeliveredCommitKeys = make(map[int]map[string]bool)

	tm.rf = raft.Make(servers, me, persister, tm.applyCh)

	tm.Killed = false
	tm.checkTimer = time.NewTimer(200 * time.Millisecond)

	go tm.runApplyOp()
	go tm.runCheckBlock()

	return tm
}

type kvClientQ struct {
	mu      sync.Mutex
	clients []*shardkv.Clerk
	cond    *sync.Cond
}

func newkvClientQ(clients []*shardkv.Clerk) *kvClientQ {
	cq := &kvClientQ{
		clients: clients,
	}

	cq.cond = sync.NewCond(&cq.mu)
	return cq
}

func (cq *kvClientQ) deQkvClient() *shardkv.Clerk {
	cq.mu.Lock()
	for len(cq.clients) == 0 {
		cq.cond.Wait()
	}

	tc := cq.clients[0]
	cq.clients = cq.clients[1:]
	cq.mu.Unlock()
	return tc
}

func (cq *kvClientQ) enQkvClient(tc *shardkv.Clerk) {
	cq.mu.Lock()
	cq.clients = append(cq.clients, tc)
	cq.cond.Broadcast()
	cq.mu.Unlock()
}

type clientQ struct {
	mu      sync.Mutex
	clients []*Clerk
	cond    *sync.Cond
}

func newClientQ(clients []*Clerk) *clientQ {
	cq := &clientQ{
		clients: clients,
	}

	cq.cond = sync.NewCond(&cq.mu)
	return cq
}

func (cq *clientQ) deQClient() *Clerk {
	cq.mu.Lock()
	for len(cq.clients) == 0 {
		cq.cond.Wait()
	}

	tc := cq.clients[0]
	cq.clients = cq.clients[1:]
	cq.mu.Unlock()
	return tc
}

func (cq *clientQ) enQClient(tc *Clerk) {
	cq.mu.Lock()
	cq.clients = append(cq.clients, tc)
	cq.cond.Broadcast()
	cq.mu.Unlock()
}
