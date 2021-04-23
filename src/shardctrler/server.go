package shardctrler


import (
	"sort"
	"6.824/raft"
	"6.824/labrpc"
	"sync"
	"6.824/labgob"
	"log"
	"time"
	"bytes"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	shard_dis map[int][]int // GID -> Shards
	unassigned_shards []int

	clientLastCmd map[int64]int64 // this will deal with duplicate command, it stores the most recent cmd for each client 
	waitingIndex map [int]chan Op	// this is the channel for sending back cmd execution for each index 

	lastIncludedIndex int
}

const (
	Join = iota
	Leave
	Move
	Query
)


type Op struct {
	// Your data here.
	OpType int 
	Servers map[int][]string
	Gids []int
	Shard int
	Gid int
	Num int
	ClientID int64
	SerialID int64
}
type OpData struct{

	ErrResult Err
	Value string 

}

func (sc *ShardCtrler) getSortedGids(incr bool) []int{

	shrd_dis := sc.shard_dis

	gids := make([]int, 0, len(shrd_dis))
	for k := range shrd_dis{
		gids = append(gids, k)
	}

	if incr{
	sort.Slice(gids, func(i, j int) bool {
		return len(shrd_dis[gids[i]]) < len(shrd_dis[gids[j]])
	})

	}else{
		sort.Slice(gids, func(i, j int) bool {
			return len(shrd_dis[gids[i]]) > len(shrd_dis[gids[j]])
		})

	}


	return gids

}


// type Config struct {
// 	Num    int              // config number
// 	Shards [NShards]int     // shard -> gid
// 	Groups map[int][]string // gid -> servers[]
// }

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here. Servers map[int][]string // new GID -> servers mappings

	sc.mu.Lock()
	defer sc.mu.Unlock()

	op := Op{

		OpType: Join,
		Servers: args.Servers,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{

		c := make(chan Op, 1)
		sc.waitingIndex[index] = c

		select {

		case opData := <- c:
			{

				log.Printf("%v", opData)

			}
		case <- time.After(1000 * time.Millisecond):

		}


		delete(sc.waitingIndex, index)


	}else{

		reply.WrongLeader = true

	}



	reply.WrongLeader = false
	currentConfig := sc.configs[len(sc.configs)-1]
	currentGruops := make(map[int][]string)
	currentShards := currentConfig.Shards
	unassigned_shards := sc.unassigned_shards

	for k, v := range currentConfig.Groups{
		currentGruops[k] = v
	}

	for k, v := range args.Servers{
		currentGruops[k] = v
		sc.shard_dis[k] = make([]int,0)
	}

	below_avg := make([]int, 0)

	majors := NShards/len(currentGruops) + 1
	num_majors := NShards % len(currentGruops)

	minors := majors - 1
	num_minors := len(currentGruops) - num_majors

	DPrintf("JOIN shard dis[%v]", sc.shard_dis)

	for k, v := range sc.shard_dis{

		if len(v)==majors && num_majors>0{
			num_majors--
		}else if len(v)==minors && num_minors>0{
			num_minors--
		}else if len(v) > majors && num_majors>0{		
			sc.shard_dis[k] = v[len(v)-majors:]
			unassigned_shards = append(unassigned_shards, v[:len(v)-majors]...)
			num_majors--
		}else if len(v) > minors && num_minors>0{
			sc.shard_dis[k] = v[len(v)-minors:]
			unassigned_shards = append(unassigned_shards, v[:len(v)-minors]...)
			num_minors--
		}else if len(v) < minors {
			below_avg = append(below_avg, k)
		}

	}

	DPrintf("JOIN Below Average[%v], majors[%v], num_majors[%v], minors[%v], num_minors[%v]",below_avg, majors, num_majors, minors, num_minors)
	DPrintf("JOIN unassignedShards[%v]", unassigned_shards)

	for _, v := range below_avg{

		num_shards := len(sc.shard_dis[v])

		if num_majors>0{

			for i:=0; i < majors-num_shards;i++{

				s := unassigned_shards[0]
				unassigned_shards =  unassigned_shards[1:]
				sc.shard_dis[v] = append(sc.shard_dis[v], s)
				currentShards[s] = v 

			}

			num_majors--

		}else if num_minors>0{

			for i:=0; i < minors-num_shards;i++{

				s := unassigned_shards[0]
				unassigned_shards =  unassigned_shards[1:]
				sc.shard_dis[v] = append(sc.shard_dis[v], s)
				currentShards[s] = v 

			}

			num_minors--

		}


	}

	newConfig := Config{
		Num: len(sc.configs),
		Shards: currentShards,
		Groups: currentGruops,
	}

	sc.unassigned_shards = unassigned_shards

	sc.configs = append(sc.configs, newConfig)

	DPrintf("JOIN Config[%v], Shrd_Dis[%v], UnassignedShrd[%v]", sc.configs, sc.shard_dis, sc.unassigned_shards)

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here. GIDs []int
	sc.mu.Lock()
	defer sc.mu.Unlock()


	op := Op{

		OpType: Leave,
		Gids: args.GIDs,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{

		c := make(chan Op, 1)
		sc.waitingIndex[index] = c


		select {

		case opData := <- c:
			{

				log.Printf("%v", opData)

			}
		case <- time.After(1000 * time.Millisecond):

		}


		delete(sc.waitingIndex, index)






	}else{

		reply.WrongLeader = true

	}



	reply.WrongLeader = false
	currentConfig := sc.configs[len(sc.configs)-1]
	currentGruops := make(map[int][]string)
	currentShards := currentConfig.Shards
	unassigned_shards := sc.unassigned_shards

	DPrintf("LEAVE START Config[%v], Shrd_Dis[%v], UnassignedShrd[%v]", sc.configs, sc.shard_dis, sc.unassigned_shards)

	for k, v := range currentConfig.Groups{
		currentGruops[k] = v
	}

	for _, gid := range args.GIDs{

		delete(currentGruops, gid)

		for _, s := range sc.shard_dis[gid]{

			unassigned_shards = append(unassigned_shards, s)

		}

		delete(sc.shard_dis, gid)

	}

	if (len(currentGruops)==0){

		newConfig := Config{
			Num: len(sc.configs),
			Shards: currentShards,
			Groups: currentGruops,
		}
	
		sc.unassigned_shards = unassigned_shards
		sc.configs = append(sc.configs, newConfig)
		return

	}

	below_avg := make([]int, 0)

	majors := NShards/len(currentGruops) + 1
	num_majors := NShards % len(currentGruops)

	minors := majors - 1
	num_minors := len(currentGruops) - num_majors

	DPrintf("LEAVE shard dis[%v]", sc.shard_dis)

	for k, v := range sc.shard_dis{

		if len(v)==majors && num_majors>0{
			num_majors--
		}else if len(v)==minors && num_minors>0{
			num_minors--
		}else if len(v) > majors && num_majors>0{		
			sc.shard_dis[k] = v[len(v)-majors:]
			unassigned_shards = append(unassigned_shards, v[:len(v)-majors]...)
			num_majors--
		}else if len(v) > minors && num_minors>0{
			sc.shard_dis[k] = v[len(v)-minors:]
			unassigned_shards = append(unassigned_shards, v[:len(v)-minors]...)
			num_minors--
		}else if len(v) < minors {
			below_avg = append(below_avg, k)
		}

	}

	DPrintf("LEAVE Below Average[%v], majors[%v], num_majors[%v], minors[%v], num_minors[%v]",below_avg, majors, num_majors, minors, num_minors)
	DPrintf("LEAVE unassignedShards[%v]", unassigned_shards)

		for _, v := range below_avg{

			num_shards := len(sc.shard_dis[v])

		if num_majors>0{

			for i:=0; i < majors-num_shards;i++{

				s := unassigned_shards[0]
				unassigned_shards =  unassigned_shards[1:]
				sc.shard_dis[v] = append(sc.shard_dis[v], s)
				currentShards[s] = v 

			}

			num_majors--

		}else if num_minors>0{


			for i:=0; i < minors-num_shards;i++{

				s := unassigned_shards[0]
				unassigned_shards =  unassigned_shards[1:]
				sc.shard_dis[v] = append(sc.shard_dis[v], s)
				currentShards[s] = v 

			}

			num_minors--

		}


	}

	newConfig := Config{
		Num: len(sc.configs),
		Shards: currentShards,
		Groups: currentGruops,
	}

	sc.unassigned_shards = unassigned_shards

	sc.configs = append(sc.configs, newConfig)
	DPrintf("LEAVE FINISHED Config[%v], Shrd_Dis[%v], UnassignedShrd[%v]", sc.configs, sc.shard_dis, sc.unassigned_shards)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	op := Op{

		OpType: Move,
		Shard: args.Shard,
		Gid: args.GID,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{

		c := make(chan Op, 1)
		sc.waitingIndex[index] = c

		select {

		case opData := <- c:
			{

				log.Printf("%v", opData)

			}
		case <- time.After(1000 * time.Millisecond):

		}


		delete(sc.waitingIndex, index)







	}else{

		reply.WrongLeader = true

	}



	reply.WrongLeader = false
	currentConfig := sc.configs[len(sc.configs)-1]
	currentGruops := make(map[int][]string)
	currentShards := currentConfig.Shards

	for k, v := range currentConfig.Groups{
		currentGruops[k] = v
	}
	original_gid := currentShards[args.Shard]

	for i, v := range sc.shard_dis[original_gid]{

		if v == args.Shard{

			sc.shard_dis[original_gid] = append(sc.shard_dis[original_gid][:i], sc.shard_dis[original_gid][i+1:]...)
			break

		}

	}

	currentShards[args.Shard] = args.GID

	newConfig := Config{
		Num: len(sc.configs),
		Shards: currentShards,
		Groups: currentGruops,
	}

	sc.configs = append(sc.configs, newConfig)


}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here. Num int 
	sc.mu.Lock()
	defer sc.mu.Unlock()

	op := Op{
		OpType: Join,
		Num: args.Num,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	index, _, isLeader := sc.rf.Start(op)

	if isLeader{

		c := make(chan Op, 1)
		sc.waitingIndex[index] = c

		select {

		case opData := <- c:
			{

				log.Printf("%v", opData)

			}
		case <- time.After(1000 * time.Millisecond):

		}


		delete(sc.waitingIndex, index)




	}else{

		reply.WrongLeader = true

	}






	DPrintf("QUERY args[%v] of config[%v]", args.Num, sc.configs)
	reply.WrongLeader = false
	if args.Num == -1 || args.Num >= len(sc.configs){

		reply.Config = sc.configs[len(sc.configs)-1]

	}else{

		reply.Config = sc.configs[args.Num]

	}


}

func (sc *ShardCtrler) executeOp(op Op){



}

//routine to listen for applied cmd from raft 
func (sc *ShardCtrler) applyChListener(){
	
	for {

		applyMsg := <- sc.applyCh

				// deal with applied snap shot
				if applyMsg.SnapshotValid{
					index := applyMsg.SnapshotIndex
					data := applyMsg.Snapshot
					storage := make(map[string]string)
					clientLastCMD := make(map[int64]int64)
					if !(index <=-1 || data == nil || len(data) < 1){ // bootstrap without any state?
						r := bytes.NewBuffer(data)
						d := labgob.NewDecoder(r)
						if d.Decode(&storage) != nil ||
						d.Decode(&clientLastCMD) != nil {
							log.Printf("error decode")
							return
						}
					}
				
					sc.clientLastCmd = clientLastCMD
					sc.lastIncludedIndex = index 
					// kv.readSnapShot()
					continue 
				}
				//ignore all non-valid cmd 
				if !applyMsg.CommandValid{
					continue 
				}
		
				op := applyMsg.Command.(Op)
				_, checkLeader := sc.rf.GetState()
				
				
				c, ok := sc.waitingIndex[applyMsg.CommandIndex]
				sc.executeOp(op)
				sc.lastIncludedIndex = applyMsg.CommandIndex
		
				if checkLeader && ok{
					c <- op
				}


	}

}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveArgs{})
	labgob.Register(MoveReply{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	sc.clientLastCmd = make(map[int64]int64)
	sc.waitingIndex = make(map[int]chan Op)
	sc.shard_dis = make(map[int][]int)
	sc.unassigned_shards = make([]int,0)
	for i := 0; i < NShards; i++{
		sc.unassigned_shards = append(sc.unassigned_shards, i)
	}
	return sc
}
