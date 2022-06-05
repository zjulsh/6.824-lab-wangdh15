package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient      logTopic = "CLNT"
	dCommit      logTopic = "CMIT"
	dDrop        logTopic = "DROP"
	dError       logTopic = "ERRO"
	dInfo        logTopic = "INFO"
	dLeader      logTopic = "LEAD"
	dLog         logTopic = "LOG1"
	dLog2        logTopic = "LOG2"
	dPersist     logTopic = "PERS"
	dSnap        logTopic = "SNAP"
	dTerm        logTopic = "TERM"
	dTest        logTopic = "TEST"
	dTimer       logTopic = "TIMR"
	dTrace       logTopic = "TRCE"
	dVote        logTopic = "VOTE"
	dWarn        logTopic = "WARN"
	dKVGet       logTopic = "KVGET"
	dKVPut       logTopic = "KVPUT"
	dKVAppend    logTopic = "KVAPP"
	dCLGet       logTopic = "CLGET"
	dCLPut       logTopic = "CLPUT"
	dCLAppend    logTopic = "CLAPP"
	dCLPutAppend logTopic = "CLPUTAPP"
)

var debugStart time.Time
var debugVerbosity int

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func DebugReceiveGet(kv *KVServer, args *GetArgs) {
	Debug(dKVGet, "S%d Receive Get Request. Arg: %v", kv.me, args)
}

func DebugReplyGet(kv *KVServer, args *GetArgs, reply *GetReply) {
	Debug(dKVGet, "S%d Reply Get Request. Args: %v, Reply: %v", kv.me, args, reply)
}

func DebugReceivePutAppend(kv *KVServer, args *PutAppendArgs) {
	if args.Op == PUT {
		Debug(dKVPut, "S%d Receive Put Request. Key: %s, Val: %s", kv.me, args.Key, args.Value)
	} else if args.Op == APPEND {
		Debug(dKVAppend, "S%d Receive APP Request. Key: %s, Val: %s", kv.me, args.Key, args.Value)
	}
}

func DebugReplyPutAppend(kv *KVServer, args *PutAppendArgs, reply *PutAppendReply) {
	if args.Op == PUT {
		Debug(dKVPut, "S%d Reply Put Request. Key: %s, Val: %s, Reply: %v", kv.me, args.Key, args.Value, reply)
	} else if args.Op == APPEND {
		Debug(dKVAppend, "S%d Reply APP Request. Key: %s, Val: %s, Reply: %v", kv.me, args.Key, args.Value, reply)
	}
}
