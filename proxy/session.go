package proxy

import (
	"log"
	"net"
	"strings"
	"time"
)

type Session interface {
	Loop(Proxy) error
	close(error)
}

type session struct {
	ts          time.Time
	ops         uint64
	microsecond uint64
	cliConn     RedisConn
	closed      bool
}

func NewSession(net net.Conn) Session {
	conn := NewConn(net, 10, 10)
	return &session{
		ts:          time.Now(),
		ops:         0,
		microsecond: 0,
		cliConn:     conn,
		closed:      false,
	}
}

func (sess *session) Loop(proxy Proxy) error {
	log.Println("new connection:", sess.remoteAddr())
	for {
		req_obj, err := sess.readReq()
		if err != nil {
			sess.close(err)
			return err
		}

		begin_time := time.Now().UnixNano()

		rlt, err := sess.exec(proxy, req_obj)
		if sess.closed {
			sess.close(err)
			return nil
		}
		if err != nil {
			sess.cliConn.writeBytes([]byte("-" + err.Error() + "\r\n"))
		} else {
			sess.cliConn.writeBytes(rlt)
		}

		end_time := time.Now().UnixNano()
		sess.ops += 1
		sess.microsecond += uint64((end_time - begin_time) / (1000))
	}
}

func (sess *session) readReq() (interface{}, error) {
	sess.cliConn.clear()
	return sess.cliConn.readReply()
}

func (sess *session) exec(proxy Proxy, req_obj interface{}) ([]byte, error) {
	req_body, ok := req_obj.([]interface{})
	if !ok || len(req_body) == 0 {
		return nil, protocolError("bad request length")
	}
	req_cmd := string(req_body[0].([]uint8))

	// handle unsupported command
	switch {
	case UnsupportedCmd(strings.ToUpper(strings.TrimSpace(req_cmd))):
		return nil, protocolError("unsupported cmd " + req_cmd)
	case strings.ToUpper(req_cmd) == "QUIT":
		sess.closed = true
		return nil, protocolError("client issue QUIT")
	case strings.ToUpper(req_cmd) == "PING":
		return []byte("+PONG\r\n"), nil
	}

	if len(req_body) < 2 {
		return nil, protocolError("only one argument given " + req_cmd)
	}
	req_key, ok := req_body[1].([]uint8)
	if !ok {
		return nil, protocolError("bad key type")
	}

	req_slot := KeySlot([]byte(req_key))
	req_bytes := sess.cliConn.getResponse()
	return proxy.slotDo(req_bytes, req_slot)
}

func (sess *session) close(err error) {
	sess.cliConn.close()
	log.Println("connection closed:",
		err.Error(),
		", create at:",
		sess.ts.Format(time.Stamp),
		", closed at:",
		time.Now().Format(time.Stamp),
		", ops:",
		sess.ops,
		", microseconds:",
		sess.microsecond,
		", remote:",
		sess.remoteAddr())
}

func (sess *session) remoteAddr() string {
	return sess.cliConn.remoteAddr()
}
