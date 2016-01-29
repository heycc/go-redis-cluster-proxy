package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"time"
)

type RedisConn interface {
	getResponse() []byte
	writeCmd(string) error
	// write raw bytes
	writeBytes([]byte) error
	// get response remote
	readReply() (interface{}, error)
	clear() error
}

// NewConn returns a new connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout int64) RedisConn {
	return &redisConn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  time.Duration(readTimeout) * time.Millisecond,
		writeTimeout: time.Duration(readTimeout) * time.Millisecond,
		response:     make([]byte, 0),
	}
}

type redisConn struct {
	conn         net.Conn
	readTimeout  time.Duration
	br           *bufio.Reader
	writeTimeout time.Duration
	bw           *bufio.Writer
	response     []byte
}

func (c *redisConn) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("response longer that buffer")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response length or line terminator")
	}
	c.bufferResponse(p)
	return p[:i], nil
}

func (c *redisConn) readLen(len int64) ([]byte, error) {
	p := make([]byte, len)
	_, err := io.ReadFull(c.br, p)
	c.bufferResponse(p)
	return p, err
}

func (c *redisConn) bufferResponse(resp []byte) error {
	c.response = append(c.response, resp...)
	return nil
}

func (c *redisConn) getResponse() []byte {
	return c.response
}

func (c *redisConn) clear() error {
	c.response = nil
	return nil
}

func parseInt(p []byte) (int64, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}
	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}
	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}
	if negate {
		n = -n
	}
	return n, nil
}

func (c *redisConn) readReply() (interface{}, error) {
	line, err := c.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		lineArr := regexp.MustCompile(" +").Split(string(line), 4)
		switch {
		// MOVED 1180 127.0.0.1:7101
		case len(lineArr) == 3 && lineArr[0] == "-MOVED":
			slot, err := strconv.ParseInt(lineArr[1], 10, 64)
			if err != nil {
				return nil, protocolError("MOVED error parse slot failed: " + err.Error())
			}
			return nil, movedError{Slot: slot, Address: lineArr[2]}
		// ASK
		case len(lineArr) == 3 && lineArr[0] == "-ASK":
			slot, err := strconv.ParseInt(lineArr[1], 10, 64)
			if err != nil {
				return nil, protocolError("ASK error parse slot failed: " + err.Error())
			}
			return nil, askError{Slot: slot, Address: lineArr[2]}
		default:
			return nil, protocolError(string(line[1:]))
		}
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseInt(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p, err := c.readLen(n)
		if err != nil {
			return nil, err
		}
		if line, err := c.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = c.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

func (c *redisConn) writeCmd(cmd string) error {
	cmdArr := regexp.MustCompile(" +").Split(cmd, 99)
	cmdStr := fmt.Sprintf("*%d\r\n", len(cmdArr))
	for _, val := range cmdArr {
		cmdStr += fmt.Sprintf("$%d\r\n", len(val))
		cmdStr += fmt.Sprintf("%s\r\n", val)
	}
	return c.writeBytes([]byte(cmdStr))
}

func (c *redisConn) writeBytes(cmd []byte) error {
	c.bw.Write(cmd)
	if err := c.bw.Flush(); err != nil {
		return protocolError("flush error")
	}
	return nil
}

func (c *redisConn) Do(cmd string) (interface{}, error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	c.writeCmd(cmd)
	reply, err := c.readReply()
	if err != nil {
		switch err := err.(type) {
		case *movedError:
			return fmt.Sprintf("moved to %s", err.Address), nil
		case *askError:
			return fmt.Sprintf("ask %s", err.Address), nil
		default:
			return nil, protocolError(fmt.Sprintf("%T", err))
		}
	} else {
		return reply, nil
	}
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)
