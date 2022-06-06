package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func CopyConfig(cfg *Config) Config {
	res := Config{}
	res.Num = cfg.Num + 1
	for i := 0; i < len(cfg.Shards); i++ {
		res.Shards[i] = cfg.Shards[i]
	}
	res.Groups = make(map[int][]string)
	for k, v := range cfg.Groups {
		res.Groups[k] = v
	}
	return res
}

func GetRandomKey(mp map[int][]string) int {
	for k := range mp {
		return k
	}
	return 0
}

// naive allocte method.
// need to optimize.
func (cfg *Config) ReAllocGID() {

	Debug(dInfo, "ReAllocGID: Shard: %v, Groups: %v", cfg.Shards, cfg.Groups)
	if len(cfg.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	// remove gids
	for i := 0; i < NShards; i++ {
		if _, ok := cfg.Groups[cfg.Shards[i]]; !ok {
			cfg.Shards[i] = 0
		}
	}

	grp2Cnt := make(map[int]int)
	for g := range cfg.Groups {
		grp2Cnt[g] = 0
	}
	for _, g := range cfg.Shards {
		if g != 0 {
			grp2Cnt[g]++
		}
	}

	avg_cnt := NShards / len(cfg.Groups)

	remain := NShards - avg_cnt*len(cfg.Groups)
	cur_remain := 0
	less_g := make([]int, 0)
	equal_avg := make([]int, 0)
	for k, v := range grp2Cnt {
		if v < avg_cnt {
			less_g = append(less_g, k)
		} else if v == avg_cnt+1 {
			cur_remain++
		} else if v == avg_cnt {
			equal_avg = append(equal_avg, k)
		}
	}

	// because itera the map is not same, so we need to sort
	sort.Ints(less_g)
	sort.Ints(equal_avg)

	// move Shard
	for i, g := range cfg.Shards {
		if g == 0 || grp2Cnt[g] > avg_cnt+1 || (grp2Cnt[g] == avg_cnt+1 && cur_remain > remain) {
			if len(less_g) != 0 {
				cfg.Shards[i] = less_g[0]
				grp2Cnt[less_g[0]]++
				if grp2Cnt[less_g[0]] == avg_cnt {
					equal_avg = append(equal_avg, less_g[0])
					less_g = less_g[1:]
				}
			} else {
				cfg.Shards[i] = equal_avg[0]
				grp2Cnt[equal_avg[0]]++
				equal_avg = equal_avg[1:]
				cur_remain++
			}
			if g != 0 {
				grp2Cnt[g]--
				if grp2Cnt[g] == avg_cnt+1 {
					cur_remain++
				} else if grp2Cnt[g] == avg_cnt {
					cur_remain--
				}
			}
		}
		Debug(dInfo, "ReAllocGID: Shard: %v, LESS: %v, EQU: %v, GCNT: %v", cfg.Shards, less_g, equal_avg, grp2Cnt)
	}
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64
	ClientSeq uint64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	ClientId  int64
	ClientSeq uint64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	ClientId  int64
	ClientSeq uint64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	ClientId  int64
	ClientSeq uint64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
