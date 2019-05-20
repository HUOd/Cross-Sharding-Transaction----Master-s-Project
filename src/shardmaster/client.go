package shardmaster

//
// Shardmaster clerk.
//

import "cst/src/labrpc"
import "time"
import "crypto/rand"
import "math/big"

var ClientsIDSet = make(map[int64]bool)

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
	_, prs := ClientsIDSet[ck.ClientID]
	for prs {
		ck.ClientID = nrand()
		_, prs = ClientsIDSet[ck.ClientID]
	}
	ClientsIDSet[ck.ClientID] = true
	ck.LastOpSeqNum = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.LastOpSeqNum++
	args := &QueryArgs{ClientID: ck.ClientID, ClientOpSeqNum: ck.LastOpSeqNum}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.LastOpSeqNum++
	args := &JoinArgs{ClientID: ck.ClientID, ClientOpSeqNum: ck.LastOpSeqNum}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.LastOpSeqNum++
	args := &LeaveArgs{ClientID: ck.ClientID, ClientOpSeqNum: ck.LastOpSeqNum}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.LastOpSeqNum++
	args := &MoveArgs{ClientID: ck.ClientID, ClientOpSeqNum: ck.LastOpSeqNum}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
