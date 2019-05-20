package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "cst/src/labrpc"
import "crypto/rand"
import "math/big"
import "cst/src/shardmaster"
import "time"

// import "fmt"

var ClientsIDSet = make(map[int64]bool)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	ClientID     int64
	LastOpSeqNum int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.ClientID = nrand()
	_, prs := ClientsIDSet[ck.ClientID]
	for prs {
		ck.ClientID = nrand()
		_, prs = ClientsIDSet[ck.ClientID]
	}
	ClientsIDSet[ck.ClientID] = true
	ck.LastOpSeqNum = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.LastOpSeqNum++
	args := GetArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     0,
	}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}

				if ok && (reply.Err == ErrWaiting) {
					si--
				}

				if ok && (reply.Err == ErrInTransaction) {
					time.Sleep(200 * time.Millisecond)
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	// return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.LastOpSeqNum++
	args := PutAppendArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     0,
	}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}

				if ok && (reply.Err == ErrWaiting) {
					si--
				}

				if ok && (reply.Err == ErrInTransaction) {
					time.Sleep(200 * time.Millisecond)
					si--
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// TGet is the Transaction Get
func (ck *Clerk) TGet(key string, TransactionNum int) (string, Err, int) {
	ck.LastOpSeqNum++
	args := GetArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     TransactionNum,
	}
	args.Key = key

	ck.config = ck.sm.Query(-1)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("Client %d returned value for Transaction %d. \n", ck.ClientID, TransactionNum)
					return reply.Value, OK, gid
				}
				if ok && (reply.Err == ErrWrongGroup) {
					return "", ErrWrongGroup, gid
				}

				if ok && (reply.Err == ErrWaiting) {
					return "", ErrWrongGroup, gid
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	// return "", "", 0
}

func (ck *Clerk) TPutAppend(key string, value string, op string, TransactionNum int) (Err, int) {
	ck.LastOpSeqNum++
	args := PutAppendArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     TransactionNum,
	}
	args.Key = key
	args.Value = value
	args.Op = op

	ck.config = ck.sm.Query(-1)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return OK, gid
				}
				if ok && reply.Err == ErrWrongGroup {
					return ErrWrongGroup, gid
				}

				if ok && (reply.Err == ErrWaiting) {
					return ErrWrongGroup, gid
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) TPut(key string, value string, TransactionNum int) (Err, int) {
	return ck.TPutAppend(key, value, "Put", TransactionNum)
}
func (ck *Clerk) TAppend(key string, value string, TransactionNum int) (Err, int) {
	return ck.TPutAppend(key, value, "Append", TransactionNum)
}

func (ck *Clerk) Prepare(key string, TransactionNum int) (bool, Err) {
	ck.LastOpSeqNum++
	args := PrepareArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     TransactionNum,
	}
	args.Key = key

	ck.config = ck.sm.Query(-1)

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PrepareReply
				ok := srv.Call("ShardKV.Prepare", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return true, OK
				}
				if ok && reply.Err == ErrWrongGroup {
					return false, ErrWrongGroup
				}
				if ok && reply.WrongLeader == false && reply.Err != OK {
					return false, reply.Err
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Commit(key string, TransactionNum int) (Err, bool) {
	ck.LastOpSeqNum++
	args := CommitArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     TransactionNum,
	}
	args.Key = key

	ck.config = ck.sm.Query(-1)

	for {
		shard := key2shard(key)
		// var gid int
		// if gid = ck.config.Shards[shard]; gid != 0 {
		// 	gid = ck.config.Shards[shard]
		// } else {
		// 	ck.config = ck.sm.Query(-1)
		// 	gid = ck.config.Shards[shard]
		// }
		gid := ck.config.Shards[shard]

		DPrintf("Client %d try to send Commit for Transaction %d to gid %d. \n", ck.ClientID, TransactionNum, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply CommitReply
				ok := srv.Call("ShardKV.Commit", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return OK, true
				}
			}
		}
		time.Sleep(100 * time.Millisecond)

		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Abort(key string, TransactionNum int) (bool, Err) {

	ck.LastOpSeqNum++
	args := AbortArgs{
		ClientID:           ck.ClientID,
		ClientLastOpSeqNum: ck.LastOpSeqNum,
		TransactionNum:     TransactionNum,
	}

	ck.config = ck.sm.Query(-1)

	shard := key2shard(key)

	for {
		gid := ck.config.Shards[shard]
		DPrintf("Client %d try to send Abort for Transaction %d to gid %d. \n", ck.ClientID, TransactionNum, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply AbortReply
				ok := srv.Call("ShardKV.Abort", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Done {
					return true, OK
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
