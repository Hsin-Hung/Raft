package shardctrler


import (
	"sort"
	"6.824/raft"
	"6.824/labrpc"
	"sync"
	"6.824/labgob"
	"log"
	"time"
)

const Debug = false

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
	waitingIndex map [int]chan OpData	// this is the channel for sending back cmd execution for each index 

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
	Config      Config
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

func (sc *ShardCtrler) exeJoin(op Op) OpData{

	opData := OpData{
		ErrResult: ErrWrongLeader,
	}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){

		//log.Printf("JOIN op[%v]    shardDis[%v]", op, sc.shard_dis)
		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID

		currentConfig := sc.configs[len(sc.configs)-1]
		currentGruops := make(map[int][]string)
		currentShards := currentConfig.Shards
		unassigned_shards := sc.unassigned_shards
	
		for k, v := range currentConfig.Groups{
			currentGruops[k] = v
		}
	
		for k, v := range op.Servers{
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
	
		DPrintf("JOIN Config[%v], Shrd_Dis[%v], UnassignedShrd[%v]", sc.configs[len(sc.configs)-1], sc.shard_dis, sc.unassigned_shards)

	}

	return opData

}

func (sc *ShardCtrler) exeLeave(op Op) OpData{
	opData := OpData{
		ErrResult: ErrWrongLeader,
	}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){

		//log.Printf("%v", op)

		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID
		currentConfig := sc.configs[len(sc.configs)-1]
		currentGruops := make(map[int][]string)
		currentShards := currentConfig.Shards
		unassigned_shards := sc.unassigned_shards
	
		//log.Printf("LEAVE START Config[%v], Shrd_Dis[%v], UnassignedShrd[%v]", sc.configs[len(sc.configs)-1], sc.shard_dis, sc.unassigned_shards)
	
		for k, v := range currentConfig.Groups{
			currentGruops[k] = v
		}
	
		for _, gid := range op.Gids{
	
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
			return opData
	
		}
	
		below_avg := make([]int, 0)
	
		majors := NShards/len(currentGruops) + 1
		num_majors := NShards % len(currentGruops)
	
		minors := majors - 1
		num_minors := len(currentGruops) - num_majors
	
		//log.Printf("LEAVE shard dis[%v]", sc.shard_dis)
	
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
	
		//log.Printf("LEAVE Below Average[%v], majors[%v], num_majors[%v], minors[%v], num_minors[%v]",below_avg, majors, num_majors, minors, num_minors)
		//log.Printf("LEAVE unassignedShards[%v]", unassigned_shards)
	
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
		//log.Printf("LEAVE FINISHED Config[%v], Shrd_Dis[%v], UnassignedShrd[%v]", sc.configs, sc.shard_dis, sc.unassigned_shards)

	}
	return opData
	

}
func (sc *ShardCtrler) exeMove(op Op) OpData{
	opData := OpData{
		ErrResult: ErrWrongLeader,
	}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){
		//log.Printf("MOVE op[%v], config[%v], shardDis[%v], unassign[%v]", op, sc.configs[len(sc.configs)-1], sc.shard_dis, sc.unassigned_shards)
		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID

	currentConfig := sc.configs[len(sc.configs)-1]
	currentGruops := make(map[int][]string)
	currentShards := currentConfig.Shards

	for k, v := range currentConfig.Groups{
		currentGruops[k] = v
	}
	original_gid := currentShards[op.Shard]

	for i, v := range sc.shard_dis[original_gid]{

		if v == op.Shard{

			sc.shard_dis[original_gid] = append(sc.shard_dis[original_gid][:i], sc.shard_dis[original_gid][i+1:]...)
			sc.shard_dis[op.Gid] = append(sc.shard_dis[op.Gid], op.Shard)
			break

		}

	}

	currentShards[op.Shard] = op.Gid

	newConfig := Config{
		Num: len(sc.configs),
		Shards: currentShards,
		Groups: currentGruops,
	}

	sc.configs = append(sc.configs, newConfig)
	//log.Printf("MOVE FINISHED op[%v], config[%v], shardDis[%v], unassign[%v]", op, sc.configs[len(sc.configs)-1], sc.shard_dis, sc.unassigned_shards)

}

return opData
	
}
func (sc *ShardCtrler) exeQuery(op Op) OpData{
	opData := OpData{}

	if sid, ok := sc.clientLastCmd[op.ClientID]; !ok || (ok && op.SerialID > sid){
		opData.ErrResult = OK
		sc.clientLastCmd[op.ClientID] = op.SerialID
		if op.Num == -1 || op.Num >= len(sc.configs){

			opData.Config = sc.configs[len(sc.configs)-1]
	
		}else{
	
			opData.Config = sc.configs[op.Num]
	
		}

	}

	return opData
}

// type Config struct {
// 	Num    int              // config number
// 	Shards [NShards]int     // shard -> gid
// 	Groups map[int][]string // gid -> servers[]
// }

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here. Servers map[int][]string // new GID -> servers mappings

	
	op := Op{

		OpType: Join,
		Servers: args.Servers,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{
		log.Printf("JOIN")
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()

		select {

		case opData := <- c:
			{
				reply.Err = opData.ErrResult
				reply.WrongLeader = false

			}
		case <- time.After(1000 * time.Millisecond):

		}

		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()

	}else{

		reply.WrongLeader = true

	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here. GIDs []int
	
	op := Op{

		OpType: Leave,
		Gids: args.GIDs,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{
		log.Printf("LEAVE")
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()

		select {

		case opData := <- c:
			{
				reply.Err = opData.ErrResult
				reply.WrongLeader = false
			}
		case <- time.After(1000 * time.Millisecond):

		}
		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()
	}else{

		reply.WrongLeader = true

	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	
	op := Op{

		OpType: Move,
		Shard: args.Shard,
		Gid: args.GID,
		ClientID: args.ClientID,
		SerialID: args.SerialID,

	}


	index, _, isLeader := sc.rf.Start(op)


	if isLeader{
		log.Printf("MOVE")
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()

		select {

		case opData := <- c:
			{

				reply.Err = opData.ErrResult
				reply.WrongLeader = false

			}
		case <- time.After(1000 * time.Millisecond):

		}
		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()
	}else{

		reply.WrongLeader = true

	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here. Num int 
	op := Op{
		OpType: Query,
		Num: args.Num,
		ClientID: args.ClientID,
		SerialID: args.SerialID,
	}

	index, _, isLeader := sc.rf.Start(op)

	if isLeader{
		log.Printf("QUERY")
		sc.mu.Lock()
		c := make(chan OpData, 1)
		sc.waitingIndex[index] = c
		sc.mu.Unlock()
		select {

		case opData := <- c:
			{

				reply.Err = opData.ErrResult
				reply.Config = opData.Config
				reply.WrongLeader = false

			}
		case <- time.After(5000 * time.Millisecond):
		}

		sc.mu.Lock()
		delete(sc.waitingIndex, index)
		sc.mu.Unlock()



	}else{

		reply.WrongLeader = true

	}

}

func (sc *ShardCtrler) executeOp(op Op) OpData{


	switch(op.OpType){

	case Join:
		return sc.exeJoin(op)
	case Leave:
		return sc.exeLeave(op)
	case Move:
		return sc.exeMove(op)
	case Query:
		return sc.exeQuery(op)
	default:
		log.Printf("No such Operation type !")

	}


	return OpData{}
}

//routine to listen for applied cmd from raft 
func (sc *ShardCtrler) applyChListener(){
	
	for {

		applyMsg := <- sc.applyCh

				if !applyMsg.CommandValid{
					continue 
				}
				op := applyMsg.Command.(Op)

				log.Printf("op %v",op)
				_, checkLeader := sc.rf.GetState()
				
				sc.mu.Lock()
				c, ok := sc.waitingIndex[applyMsg.CommandIndex]
				opData := sc.executeOp(op)
				sc.lastIncludedIndex = applyMsg.CommandIndex
				sc.mu.Unlock()
		
				if checkLeader && ok{
					c <- opData
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
	sc.waitingIndex = make(map[int]chan OpData)
	sc.shard_dis = make(map[int][]int)
	sc.unassigned_shards = make([]int,0)
	for i := 0; i < NShards; i++{
		sc.unassigned_shards = append(sc.unassigned_shards, i)
	}
	go sc.applyChListener()
	return sc
}
