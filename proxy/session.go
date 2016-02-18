package proxy

import (
	"log"
	"fmt"
	"net"
	"time"
)

type Session interface {
	readRequest() (uint16, error)
	Work(Proxy) error
	close()
}

type session struct {
	ts          time.Time
	ops         uint64
	microsecond uint64
	cliConn     RedisConn
}

func NewSession(net net.Conn) Session {
	conn := NewConn(net, 10, 10)
	return &session{
		ts:          time.Now(),
		ops:         0,
		microsecond: 0,
		cliConn:     conn,
	}
}

func (sess *session) readRequest() (uint16, error) {
	reply, err := sess.cliConn.readReply()
	if err != nil {
		return 0, protocolError("readRequest error " + err.Error())
	} else {
		reqBody, ok := reply.([]interface{})
		if !ok || len(reqBody) == 0 {
			return 0, protocolError("Bad request sequence")
		}
		cmd, _ := reqBody[0].([]uint8)
		if UnsupportedCmd(cmd) {
			return 0, protocolError("UnsupportedCmd " + cmd)
		}
		if len(reqBody) < 2 {
			return 0, protocolError("Bad length of cmd " + cmd)
		}
		if key, ok := reqBody[1].([]uint8); ok {
			return KeySlot([]byte(key)), nil
		} else {
			return 0, protocolError("Bad key type " + key)
		}
	}
}

func (sess *session) Work(proxy Proxy) error {
	for {
		slot, err := sess.readRequest()
		cmd := sess.cliConn.getResponse()

		if err != nil {
			sess.close()
			return nil
		}

		// handle PING & QUIT

		reply, err := proxy.slotDo(cmd, slot)
		if err != nil {
			fmt.Println("do err", err.Error())
		}
		sess.cliConn.writeBytes(reply)
		sess.cliConn.clear()
		sess.ops += 1
	}
}

func (sess *session) close() {
	log.Println("connection closed.",
		"create at:",
		sess.ts,
		"closed at:",
		time.Now(),
		"ops",
		sess.ops,
		"total time(microsecond)",
		sess.microsecond,
		"addr",
		sess.cliConn.conn.RemoteAddr())
}