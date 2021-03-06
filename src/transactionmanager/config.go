package transactionmanager

import "cst/src/shardmaster"
import "cst/src/labrpc"
import "cst/src/shardkv"
import "testing"
import "os"

// import "log"
import crand "crypto/rand"
import "math/big"
import "math/rand"
import "encoding/base64"
import "sync"
import "runtime"
import "cst/src/raft"
import "strconv"
import "fmt"
import "time"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type group struct {
	gid       int
	servers   []*shardkv.ShardKV
	saved     []*raft.Persister
	endnames  [][]string
	mendnames [][]string
}

type config struct {
	mu    sync.Mutex
	t     *testing.T
	b     *testing.B
	net   *labrpc.Network
	start time.Time // time at which make_config() was called

	nmasters      int
	masterservers []*shardmaster.ShardMaster
	mck           *shardmaster.Clerk

	ngroups int
	n       int // servers per k/v group
	groups  []*group

	ShardKVclerks map[*shardkv.Clerk][]string
	nextClientId  int
	maxraftstate  int

	ntmanagers      int
	tmanagerservers []*TransactionManager
	tmanagerck      *Clerk
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		cfg.ShutdownGroup(gi)
	}
	cfg.net.Cleanup()
	// cfg.checkTimeout()
}

// check that no server's log is too big.
func (cfg *config) checklogs() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			if cfg.maxraftstate >= 0 && raft > 2*cfg.maxraftstate {
				cfg.t.Fatalf("persister.RaftStateSize() %v, but maxraftstate %v",
					raft, cfg.maxraftstate)
			}
			if cfg.maxraftstate < 0 && snap > 0 {
				cfg.t.Fatalf("maxraftstate is -1, but snapshot is non-empty!")
			}
		}
	}
}

// master server name for labrpc.
func (cfg *config) mastername(i int) string {
	return "master" + strconv.Itoa(i)
}

// shard server name for labrpc.
// i'th server of group gid.
func (cfg *config) servername(gid int, i int) string {
	return "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(i)
}

// manager server name of labrpc
func (cfg *config) tmanagername(i int) string {
	return "tmanager" + strconv.Itoa(i)
}

func (cfg *config) makeShardKVClient() *shardkv.Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// ClientEnds to talk to master service.
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	endnames := make([]string, cfg.n)
	for j := 0; j < cfg.nmasters; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], cfg.mastername(j))
		cfg.net.Enable(endnames[j], true)
	}

	ck := shardkv.MakeClerk(ends, func(servername string) *labrpc.ClientEnd {
		name := randstring(20)
		end := cfg.net.MakeEnd(name)
		cfg.net.Connect(name, servername)
		cfg.net.Enable(name, true)
		return end
	})
	cfg.ShardKVclerks[ck] = endnames
	cfg.nextClientId++
	return ck
}

func (cfg *config) deleteShardKVClient(ck *shardkv.Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.ShardKVclerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.ShardKVclerks, ck)
}

// Shutdown i'th server of gi'th group, by isolating it
func (cfg *config) ShutdownServer(gi int, i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	gg := cfg.groups[gi]

	// prevent this server from sending
	for j := 0; j < len(gg.servers); j++ {
		name := gg.endnames[i][j]
		cfg.net.Enable(name, false)
	}
	for j := 0; j < len(gg.mendnames[i]); j++ {
		name := gg.mendnames[i][j]
		cfg.net.Enable(name, false)
	}

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(cfg.servername(gg.gid, i))

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	}

	kv := gg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		gg.servers[i] = nil
	}
}

func (cfg *config) ShutdownGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.ShutdownServer(gi, i)
	}
}

// start i'th server in gi'th group
func (cfg *config) StartKVServer(gi int, i int) {
	cfg.mu.Lock()

	gg := cfg.groups[gi]

	// a fresh set of outgoing ClientEnd names
	// to talk to other servers in this group.
	gg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		gg.endnames[i][j] = randstring(20)
	}

	// and the connections to other servers in this group.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
		cfg.net.Connect(gg.endnames[i][j], cfg.servername(gg.gid, j))
		cfg.net.Enable(gg.endnames[i][j], true)
	}

	// ends to talk to shardmaster service
	mends := make([]*labrpc.ClientEnd, cfg.nmasters)
	gg.mendnames[i] = make([]string, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		gg.mendnames[i][j] = randstring(20)
		mends[j] = cfg.net.MakeEnd(gg.mendnames[i][j])
		cfg.net.Connect(gg.mendnames[i][j], cfg.mastername(j))
		cfg.net.Enable(gg.mendnames[i][j], true)
	}

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartKVServer()
	// the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	} else {
		gg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	gg.servers[i] = shardkv.StartServer(ends, i, gg.saved[i], cfg.maxraftstate,
		gg.gid, mends,
		func(servername string) *labrpc.ClientEnd {
			name := randstring(20)
			end := cfg.net.MakeEnd(name)
			cfg.net.Connect(name, servername)
			cfg.net.Enable(name, true)
			return end
		})

	kvsvc := labrpc.MakeService(gg.servers[i])
	rfsvc := labrpc.MakeService(gg.servers[i].Rf)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.servername(gg.gid, i), srv)
}

func (cfg *config) StartGroup(gi int) {
	for i := 0; i < cfg.n; i++ {
		cfg.StartKVServer(gi, i)
	}
}

func (cfg *config) StartMasterServer(i int) {
	// ClientEnds to talk to other master replicas.
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, cfg.mastername(j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	cfg.masterservers[i] = shardmaster.StartServer(ends, i, p)

	msvc := labrpc.MakeService(cfg.masterservers[i])
	rfsvc := labrpc.MakeService(cfg.masterservers[i].Raft())
	srv := labrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.mastername(i), srv)
}

func (cfg *config) shardmasterclerk() *shardmaster.Clerk {
	// ClientEnds to talk to master service.
	ends := make([]*labrpc.ClientEnd, cfg.nmasters)
	for j := 0; j < cfg.nmasters; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.mastername(j))
		cfg.net.Enable(name, true)
	}

	return shardmaster.MakeClerk(ends)
}

func (cfg *config) StartTManagerServer(i int, kvClientNum int) {
	// ClientEnds to talk to other master replicas.
	ends := make([]*labrpc.ClientEnd, cfg.ntmanagers)
	for j := 0; j < cfg.ntmanagers; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, cfg.tmanagername(j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	kvclients := make([]*shardkv.Clerk, kvClientNum)

	for i := 0; i < len(kvclients); i++ {
		kvc := cfg.makeShardKVClient()
		kvclients[i] = kvc
	}

	cfg.tmanagerservers[i] = StartServer(ends, i, p, kvclients)

	msvc := labrpc.MakeService(cfg.tmanagerservers[i])
	rfsvc := labrpc.MakeService(cfg.tmanagerservers[i].Raft())
	srv := labrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.tmanagername(i), srv)
}

func (cfg *config) tmanagerclerk() *Clerk {
	// ClientEnds to talk to master service.
	ends := make([]*labrpc.ClientEnd, cfg.ntmanagers)
	for j := 0; j < cfg.ntmanagers; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.tmanagername(j))
		cfg.net.Enable(name, true)
	}

	return MakeClerk(ends)
}

// tell the shardmaster that a group is joining.
func (cfg *config) join(gi int) {
	cfg.joinm([]int{gi})
}

func (cfg *config) joinm(gis []int) {
	m := make(map[int][]string, len(gis))
	for _, g := range gis {
		gid := cfg.groups[g].gid
		servernames := make([]string, cfg.n)
		for i := 0; i < cfg.n; i++ {
			servernames[i] = cfg.servername(gid, i)
		}
		m[gid] = servernames
	}
	cfg.mck.Join(m)
}

// tell the shardmaster that a group is leaving.
func (cfg *config) leave(gi int) {
	cfg.leavem([]int{gi})
}

func (cfg *config) leavem(gis []int) {
	gids := make([]int, 0, len(gis))
	for _, g := range gis {
		gids = append(gids, cfg.groups[g].gid)
	}
	cfg.mck.Leave(gids)
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool, maxraftstate int) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()
	cfg.start = time.Now()

	// master
	cfg.nmasters = 3
	cfg.masterservers = make([]*shardmaster.ShardMaster, cfg.nmasters)
	for i := 0; i < cfg.nmasters; i++ {
		cfg.StartMasterServer(i)
	}
	cfg.mck = cfg.shardmasterclerk()

	cfg.n = n
	cfg.ShardKVclerks = make(map[*shardkv.Clerk][]string)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid

	// transactionmanager
	cfg.ntmanagers = 3
	cfg.tmanagerservers = make([]*TransactionManager, cfg.ntmanagers)
	for i := 0; i < cfg.ntmanagers; i++ {
		cfg.StartTManagerServer(i, 10)
	}
	cfg.tmanagerck = cfg.tmanagerclerk()

	cfg.ngroups = 3
	cfg.groups = make([]*group, cfg.ngroups)
	// cfg.n = n
	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*shardkv.ShardKV, cfg.n)
		gg.saved = make([]*raft.Persister, cfg.n)
		gg.endnames = make([][]string, cfg.n)
		gg.mendnames = make([][]string, cfg.nmasters)
		for i := 0; i < cfg.n; i++ {
			cfg.StartKVServer(gi, i)
		}
	}

	cfg.net.Reliable(!unreliable)

	return cfg
}

func make_config_b(b *testing.B, n int, unreliable bool, maxraftstate int) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.b = b
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()
	cfg.start = time.Now()

	// master
	cfg.nmasters = 3
	cfg.masterservers = make([]*shardmaster.ShardMaster, cfg.nmasters)
	for i := 0; i < cfg.nmasters; i++ {
		cfg.StartMasterServer(i)
	}
	cfg.mck = cfg.shardmasterclerk()

	cfg.n = n
	cfg.ShardKVclerks = make(map[*shardkv.Clerk][]string)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid

	// transactionmanager
	cfg.ntmanagers = 3
	cfg.tmanagerservers = make([]*TransactionManager, cfg.ntmanagers)
	for i := 0; i < cfg.ntmanagers; i++ {
		cfg.StartTManagerServer(i, 10)
	}
	cfg.tmanagerck = cfg.tmanagerclerk()

	cfg.ngroups = 3
	cfg.groups = make([]*group, cfg.ngroups)
	// cfg.n = n
	for gi := 0; gi < cfg.ngroups; gi++ {
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*shardkv.ShardKV, cfg.n)
		gg.saved = make([]*raft.Persister, cfg.n)
		gg.endnames = make([][]string, cfg.n)
		gg.mendnames = make([][]string, cfg.nmasters)
		for i := 0; i < cfg.n; i++ {
			cfg.StartKVServer(gi, i)
		}
	}

	cfg.net.Reliable(!unreliable)

	return cfg
}
