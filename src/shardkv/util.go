package shardkv

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dCLGet       logTopic = "CLGET"
	dCLPut       logTopic = "CLPUT"
	dCLAppend    logTopic = "CLAPP"
	dCLPutAppend logTopic = "CLPUTAPP"
	dCLConfig    logTopic = "CLCFG"
	dKVGet       logTopic = "KVGET"
	dKVPut       logTopic = "KVPUT"
	dKVAppend    logTopic = "KVAPP"
	dInfo        logTopic = "INFO"
	dKVConfig    logTopic = "KVCFG"
	dKVShard     logTopic = "KVSHD"
	dKVApply     logTopic = "KVAPLY"
	dError       logTopic = "KVERROR"
	dKVSnapshot  logTopic = "KVSNAPSHOT"
	dKVGC        logTopic = "KVGC"
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
