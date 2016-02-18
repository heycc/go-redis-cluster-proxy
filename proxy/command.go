package proxy

const cmd_deny = []string{
	"CLUSTER","READONLY","READWRITE",
	"AUTH","ECHO","PING","QUIT","SELECT",
	"PFADD","PFCOUNT","PFMERGE",
	"BLPOP","BRPOP","BRPOPLPUSH","RPOPLPUSH",
	"PSUBSCRIBE","PUBSUB","PUBLISH","PUNSUBSCRIBE","SUBSCRIBE","UNSUBSCRIBE",
	"EVAL","EVALSHA","SCRIPT",
	"BGREWRITEAOF","BGSAVE","CLIENT","COMMAND","CONFIG","DBSIZE","DEBUG",
	"FLUSHALL","FLUSHDB","INFO","LASTSAVE","MONITOR","ROLE","SAVE","SHUTDOWN","SLAVEOF","SLOWLOG","SYNC","TIME",
	"SDIFF","SDIFFSTORE","SINTER","SINTERSTORE","SMOVE","SUNION","SUNIONSTORE",
	"ZINTERSTORE","ZUNIONSTORE","BITOP"
}

func UnsupportedCmd(cmd string) bool {
	cmd = string.ToUpper(strings.TrimSpace(cmd))
	for _, c := range cmd_deny {
		if c == cmd {
			return true
		}
	}
	return false
}