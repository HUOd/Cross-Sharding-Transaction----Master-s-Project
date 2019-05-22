package shardmaster

import "cst/src/raft"
import "cst/src/labrpc"
import "sync"
import "cst/src/labgob"
import "log"
import "time"

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	Killed bool

	// Your data here.
	ClientLastOpSeqNum map[int64]int
	CallbackChanMap    map[int]chan struct{}
	configs            []Config // indexed by config num
}

type Op struct {
	OpName         string // Join/Leave/Move/Query
	ClientID       int64
	ClientOpSeqNum int
	// args
	// Join arg
	JoinServers map[int][]string
	// Query arg
	QueryConfigNum int
	// Leave arg
	LeaveGIDs []int
	// Move arg
	MoveShard int
	MoveGID   int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {

	DPrintf("ShardMaster %d received Join request from client %d \n", sm.me, args.ClientID)

	thisOperation := Op{
		OpName:         JOIN,
		ClientID:       args.ClientID,
		ClientOpSeqNum: args.ClientOpSeqNum,
		JoinServers:    copyGroupsMap(args.Servers),
	}

	newIndex, thisTerm, isLeader := sm.rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardMaster %d is leader, in track of Join at index %d. \n", sm.me, newIndex)
		sm.mu.Lock()
		mchan := make(chan struct{})
		sm.CallbackChanMap[newIndex] = mchan
		sm.mu.Unlock()

		for {
			select {
			case <-mchan:
				DPrintf("ShardMaster %d received Join apply callback at index %d. \n", sm.me, newIndex)
				if curTerm, isLeader := sm.rf.GetState(); isLeader && curTerm == thisTerm {
					reply.Err = OK
				} else {
					DPrintf("ShardMaster %d is no longer leader, reject Join request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
				}

				return

			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader = sm.rf.GetState(); !isLeader {
					sm.mu.Lock()
					delete(sm.CallbackChanMap, newIndex)
					sm.mu.Unlock()
					DPrintf("ShardMaster %d is no longer leader, reject Join request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
					return
				}
				continue
			}
		}
	} else {
		reply.WrongLeader = true
		DPrintf("ShardMaster %d is not leader, reject Join request from client %d \n", sm.me, args.ClientID)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {

	DPrintf("ShardMaster %d received Leave request from client %d \n", sm.me, args.ClientID)
	leaveGIDs := make([]int, len(args.GIDs))
	copy(leaveGIDs, args.GIDs)

	thisOperation := Op{
		OpName:         LEAVE,
		ClientID:       args.ClientID,
		ClientOpSeqNum: args.ClientOpSeqNum,
		LeaveGIDs:      leaveGIDs,
	}

	newIndex, thisTerm, isLeader := sm.rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardMaster %d is leader, in track of Leave at index %d. \n", sm.me, newIndex)
		sm.mu.Lock()
		mchan := make(chan struct{})
		sm.CallbackChanMap[newIndex] = mchan
		sm.mu.Unlock()

		for {
			select {
			case <-mchan:
				DPrintf("ShardMaster %d received Leave apply callback at index %d. \n", sm.me, newIndex)
				if curTerm, isLeader := sm.rf.GetState(); isLeader && curTerm == thisTerm {
					reply.Err = OK
				} else {
					DPrintf("ShardMaster %d is no longer leader, reject Leave request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
				}

				return

			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader = sm.rf.GetState(); !isLeader {
					sm.mu.Lock()
					delete(sm.CallbackChanMap, newIndex)
					sm.mu.Unlock()
					DPrintf("ShardMaster %d is no longer leader, reject Leave request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
					return
				}
				continue
			}
		}
	} else {
		reply.WrongLeader = true
		DPrintf("ShardMaster %d is not leader, reject Leave request from client %d \n", sm.me, args.ClientID)
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {

	DPrintf("ShardMaster %d received Move request from client %d \n", sm.me, args.ClientID)

	thisOperation := Op{
		OpName:         MOVE,
		ClientID:       args.ClientID,
		ClientOpSeqNum: args.ClientOpSeqNum,
		MoveGID:        args.GID,
		MoveShard:      args.Shard,
	}

	newIndex, thisTerm, isLeader := sm.rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardMaster %d is leader, in track of Move at index %d. \n", sm.me, newIndex)
		sm.mu.Lock()
		mchan := make(chan struct{})
		sm.CallbackChanMap[newIndex] = mchan
		sm.mu.Unlock()

		for {
			select {
			case <-mchan:
				DPrintf("ShardMaster %d received Move apply callback at index %d. \n", sm.me, newIndex)
				if curTerm, isLeader := sm.rf.GetState(); isLeader && curTerm == thisTerm {
					reply.Err = OK
				} else {
					DPrintf("ShardMaster %d is no longer leader, reject Move request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
				}

				return

			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader = sm.rf.GetState(); !isLeader {
					sm.mu.Lock()
					delete(sm.CallbackChanMap, newIndex)
					sm.mu.Unlock()
					DPrintf("ShardMaster %d is no longer leader, reject Move request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
					return
				}
				continue
			}
		}
	} else {
		reply.WrongLeader = true
		DPrintf("ShardMaster %d is not leader, reject Move request from client %d \n", sm.me, args.ClientID)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("ShardMaster %d received Query request from client %d \n", sm.me, args.ClientID)

	thisOperation := Op{
		OpName:         QUERY,
		ClientID:       args.ClientID,
		ClientOpSeqNum: args.ClientOpSeqNum,
		QueryConfigNum: args.Num,
	}

	newIndex, thisTerm, isLeader := sm.rf.Start(thisOperation)

	if isLeader {
		DPrintf("ShardMaster %d is leader, in track of Query at index %d. \n", sm.me, newIndex)
		sm.mu.Lock()
		mchan := make(chan struct{})
		sm.CallbackChanMap[newIndex] = mchan
		sm.mu.Unlock()

		for {
			select {
			case <-mchan:
				DPrintf("ShardMaster %d received Query apply callback at index %d. \n", sm.me, newIndex)
				if curTerm, isLeader := sm.rf.GetState(); isLeader && curTerm == thisTerm {
					sm.mu.Lock()
					if args.Num < 0 || args.Num >= len(sm.configs) {
						reply.Config = sm.configs[len(sm.configs)-1]
					} else {
						reply.Config = sm.configs[args.Num]
					}
					sm.mu.Unlock()
					reply.Err = OK
				} else {
					DPrintf("ShardMaster %d is no longer leader, reject Query request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
				}

				return

			case <-time.After(raft.HEART_BEAT_INTERVAL * time.Millisecond):
				if _, isLeader = sm.rf.GetState(); !isLeader {
					sm.mu.Lock()
					delete(sm.CallbackChanMap, newIndex)
					sm.mu.Unlock()
					DPrintf("ShardMaster %d is no longer leader, reject Query request from client %d \n", sm.me, args.ClientID)
					reply.WrongLeader = true
					return
				}
				continue
			}
		}
	} else {
		reply.WrongLeader = true
		DPrintf("ShardMaster %d is not leader, reject Query request from client %d \n", sm.me, args.ClientID)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	DPrintf("ShardMaster %d has been ---- Killed ----! \n", sm.me)
	sm.rf.Kill()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) runApplyOp() {
	for {
		am := <-sm.applyCh

		if am.CommandValid && am.Command != nil {
			operation := am.Command.(Op)

			sm.mu.Lock()

			if operation.OpName != QUERY {

				lsn, prs := sm.ClientLastOpSeqNum[operation.ClientID]

				if !prs || (prs && lsn < operation.ClientOpSeqNum) {
					switch operation.OpName {
					case JOIN:
						sm.joinGroups(operation.JoinServers)
					case LEAVE:
						sm.leaveGroups(operation.LeaveGIDs)
					case MOVE:
						sm.moveShardAndGroup(operation.MoveGID, operation.MoveShard)
					default:
						DPrintf("ShardMaster %d received %v operation at index %d, not a normal one \n", sm.me, operation.OpName, am.CommandIndex)
					}

					sm.ClientLastOpSeqNum[operation.ClientID] = operation.ClientOpSeqNum

				} else {
					DPrintf("ShardMaster %d wont apply Op at index %d from client %d. \n", sm.me, am.CommandIndex, operation.ClientID)
				}
			}

			if cbChan, prs := sm.CallbackChanMap[am.CommandIndex]; prs && cbChan != nil {
				close(cbChan)
				delete(sm.CallbackChanMap, am.CommandIndex)
				DPrintf("ShardMaster %d sent back to chan, at index %d from client %d. \n", sm.me, operation.ClientID)
			}

			sm.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.CallbackChanMap = make(map[int]chan struct{})
	sm.ClientLastOpSeqNum = make(map[int64]int)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.Killed = false

	go sm.runApplyOp()

	DPrintf("ShardMaster %d has been ****Started!**** !\n", sm.me)

	return sm
}

func copyGroupsMap(om map[int][]string) map[int][]string {
	nm := make(map[int][]string)
	for k, v := range om {
		nm[k] = v
	}
	return nm
}

func (sm *ShardMaster) getConfig(index int) Config {

	res := Config{}
	if index < 0 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}

	res.Num = sm.configs[index].Num
	res.Shards = sm.configs[index].Shards
	res.Groups = copyGroupsMap(sm.configs[index].Groups)
	return res
}

// Rebalance the shards through the gid
func (sm *ShardMaster) balanceShards(latestConfig *Config) {
	listOfGID := make([]int, 0)
	for k := range latestConfig.Groups {
		listOfGID = append(listOfGID, k)
	}

	numOfGroups := len(listOfGID)

	if numOfGroups == 0 {
		for i := range latestConfig.Shards {
			latestConfig.Shards[i] = 0
		}

		return

	}

	shardForOne := NShards / numOfGroups

	if shardForOne == 0 {
		shardForOne = 1
	}

	indexOfListGID := 0
	i := 0
	j := 0
	for i < NShards && indexOfListGID < numOfGroups {
		if j < shardForOne {
			latestConfig.Shards[i] = listOfGID[indexOfListGID]
			i++
			j++
		} else if j == shardForOne {
			indexOfListGID++
			j = 0
		}
	}

	for i < NShards {
		latestConfig.Shards[i] = listOfGID[len(listOfGID)-1]
		i++
	}
}

func (sm *ShardMaster) joinGroups(newServers map[int][]string) {
	if newServers == nil {
		return
	}

	numOfNew := len(newServers)

	if numOfNew == 0 {
		return
	}

	latestConfig := sm.getConfig(-1)

	for k, v := range newServers {
		latestConfig.Groups[k] = v
	}

	sm.balanceShards(&latestConfig)

	latestConfig.Num++
	sm.configs = append(sm.configs, latestConfig)
}

func (sm *ShardMaster) leaveGroups(GIDs []int) {
	if len(GIDs) == 0 {
		return
	}

	latestConfig := sm.getConfig(-1)

	count := 0
	for i := range GIDs {
		if _, prs := latestConfig.Groups[GIDs[i]]; prs {
			delete(latestConfig.Groups, GIDs[i])
			count++
		}
	}

	if count == 0 {
		return
	}

	sm.balanceShards(&latestConfig)

	latestConfig.Num++
	sm.configs = append(sm.configs, latestConfig)
}

func (sm *ShardMaster) moveShardAndGroup(GID, Shard int) {
	latestConfig := sm.getConfig(-1)
	latestConfig.Shards[Shard] = GID

	latestConfig.Num++
	sm.configs = append(sm.configs, latestConfig)
}
