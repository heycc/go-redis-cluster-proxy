package proxy

import (
	"fmt"
	"net"
	"time"
)

type Session interface {
	Work(Proxy) error
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
		return 0, Error("readRequest error " + err.Error())
	} else {
		reqBody, _ := reply.([]interface{})
		key, _ := reqBody[1].(string)
		return KeySlot([]byte(key)), nil
	}
}

func (sess *session) Work(proxy Proxy) error {
	// fmt.Println("33", proxy)
	// proxy.GetAddr()
	// fmt.Println(sess.cliConn.conn.RemoteAddr(), "Exec")
	for {
		slot, err := sess.readRequest()
		cmd := sess.cliConn.getResponse()

		if err != nil {
			// fmt.Println(sess.cliConn.conn.RemoteAddr(), "error", err.Error())
			return nil
		}

		// fmt.Println(sess.cliConn.conn.RemoteAddr(), "requst", cmd)
		reply, err := proxy.slotDo(cmd, slot)
		if err != nil {
			fmt.Println("do err", err.Error())
		}
		sess.cliConn.writeBytes(reply)
		sess.cliConn.clear()
	}
}
