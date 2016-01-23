package proxy

import (
	"net"
	"fmt"
	"time"
)

type Session interface {
	Exec(Proxy) error
}

type session struct {
	ts	time.Time
	ops	uint64
	microsecond	uint64
	conn Conn
}

func NewSession (net net.Conn) Session {
	conn := NewConn(net, 10, 10)
	return &session{
		ts: time.Now(),
		ops: 0,
		microsecond: 0,
		conn: conn,
	}
}

func (sess *session) readRequest() error {
	reply, err := sess.conn.readReply()
	if err != nil {
		fmt.Println(err.Error())
		return Error("readRequest error " + err.Error())
	} else {
		if reply, ok := reply.([]interface{}); ok {
			for _, ele := range reply {
				if ele, ok := ele.([]uint8); ok {
					fmt.Println("in req", string(ele))
				} else {
					fmt.Println("in req", ele)
				}
			}
			return nil
		} else {
			return Error("decode reply error")
		}
	}
}

func (sess *session) Exec(proxy Proxy) error {
	for {
		if ok := sess.readRequest(); ok != nil {
			sess.conn.writeBytes([]byte("-readRequestFailed"))
		}
		command := sess.conn.getResponse()
		reply, err := proxy.Do(command)
		if err != nil {
			reply = []byte("-CCERR")
		}
		sess.conn.writeBytes(reply)
		sess.conn.clear()
	}
}