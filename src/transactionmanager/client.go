package transactionmanager

import "cst/src/labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "sync"

var ClientsIDSet = make(map[int64]bool)
var gm sync.Mutex

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientID     int64
	LastOpSeqNum int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.ClientID = nrand()
	gm.Lock()
	_, prs := ClientsIDSet[ck.ClientID]
	gm.Unlock()
	for prs {
		ck.ClientID = nrand()
		gm.Lock()
		_, prs = ClientsIDSet[ck.ClientID]
		gm.Unlock()
	}
	gm.Lock()
	ClientsIDSet[ck.ClientID] = true
	gm.Unlock()
	ck.LastOpSeqNum = 0

	return ck
}

func (ck *Clerk) Get(Key string, TransactionID int) (string, Err) {
	ck.LastOpSeqNum++
	args := &GetArgs{
		Key:                Key,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionID:      TransactionID,
	}

	for {
		for _, srv := range ck.servers {
			var reply GetReply
			ok := srv.Call("TransactionManager.Get", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return reply.Value, OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return "", reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) PutAppend(Key string, Value string, Op string, TransactionID int) Err {
	ck.LastOpSeqNum++
	args := &PutAppendArgs{
		Key:                Key,
		Value:              Value,
		Op:                 Op,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionID:      TransactionID,
	}

	for {
		for _, srv := range ck.servers {
			var reply PutAppendReply
			ok := srv.Call("TransactionManager.PutAppend", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, TransactionID int) Err {
	return ck.PutAppend(key, value, "Put", TransactionID)
}

func (ck *Clerk) Append(key string, value string, TransactionID int) Err {
	return ck.PutAppend(key, value, "Append", TransactionID)
}

func (ck *Clerk) Begin() (int, Err) {
	ck.LastOpSeqNum++
	args := &BeginArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
	}

	for {
		for _, srv := range ck.servers {
			var reply BeginReply
			ok := srv.Call("TransactionManager.Begin", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return reply.TransactionID, OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return -1, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Commit(TransactionID int) Err {
	ck.LastOpSeqNum++
	args := &CommitArgs{
		TransactionID:      TransactionID,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
	}

	for {
		for _, srv := range ck.servers {
			var reply CommitReply
			ok := srv.Call("TransactionManager.Commit", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Abort(TransactionID int) Err {
	ck.LastOpSeqNum++
	args := &AbortArgs{
		TransactionID:      TransactionID,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
	}

	for {
		for _, srv := range ck.servers {
			var reply AbortReply
			ok := srv.Call("TransactionManager.Abort", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Update(TransactionID int, Key string, Gid int) Err {
	ck.LastOpSeqNum++
	args := &UpdateArgs{
		Key:                Key,
		Gid:                Gid,
		TransactionID:      TransactionID,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
	}

	for {
		for _, srv := range ck.servers {
			var reply UpdateReply
			ok := srv.Call("TransactionManager.Update", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) UpdateState(TransactionID int, State TsState) Err {
	ck.LastOpSeqNum++
	args := &UpdateStateArgs{
		State:              State,
		TransactionID:      TransactionID,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
	}

	for {
		for _, srv := range ck.servers {
			var reply UpdateStateReply
			ok := srv.Call("TransactionManager.UpdateState", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) AddBlockKey(TransactionID int, Key string, Op string) Err {
	ck.LastOpSeqNum++
	args := &AddBlockingKeyArgs{
		TransactionNum:     TransactionID,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		Key:                Key,
		BlockingType:       Op,
	}

	for {
		for _, srv := range ck.servers {
			var reply AddBlockingKeyReply
			ok := srv.Call("TransactionManager.AddBlockingKey", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) RemoveBlockKey(TransactionID int, Key string, Op string) Err {
	ck.LastOpSeqNum++
	args := &RemoveBlockingKeyArgs{
		TransactionNum:     TransactionID,
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		Key:                Key,
		BlockingType:       Op,
	}

	for {
		for _, srv := range ck.servers {
			var reply RemoveBlockingKeyReply
			ok := srv.Call("TransactionManager.RemoveBlockingKey", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				return OK
			}

			if ok && reply.WrongLeader == false && reply.Err != OK {
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
