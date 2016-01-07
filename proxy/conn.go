package proxy

import (
	"bufio"
	//"bytes"
	//"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	//"sync"
	"time"
)

type conn struct {
	conn    net.Conn
	// Read
	readTimeout time.Duration
	br          *bufio.Reader
	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer

	command		[]byte
	response	[]byte
}

// NewConn returns a new connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout int64) Conn {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  time.Duration(readTimeout)*time.Millisecond,
		writeTimeout: time.Duration(readTimeout)*time.Millisecond,
		command:	make([]byte, 0),
		response:	make([]byte, 0),
	}
}

func (c *conn) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	c.bufferResponse(p)
	return p[:i], nil
}

func (c *conn) readLen(len int64) ([]byte, error) {
	p := make([]byte, len)
	_, err := io.ReadFull(c.br, p)
	c.bufferResponse(p)
	return p, err
}

func (c *conn) bufferResponse(resp []byte) error {
	// buffer response
	c.response = append(c.response, resp...)
	return nil
}

func (c *conn) GetResponse() []byte {
	return c.response
}

func parseLen(p []byte) (int64, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}
	return strconv.ParseInt(string(p), 10, 64)
}

func parseInt(p []byte) (int64, error) {
	return strconv.ParseInt(string(p), 10, 64)
}

func (c *conn) readReply() (interface{}, error) {
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
			// Avoid allocation for frequent "+OK" response.
			return okReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
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

func (c *conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	c.bw.Write([]byte(cmd))
	if err := c.bw.Flush(); err != nil {
		return nil, protocolError("flush error")
	}
	// return p, nil
	var reply interface{}
	var e error
	if reply, e = c.readReply(); e != nil {
		return nil, protocolError("readReply error")
	}
	
	return reply, nil
}


type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("proxy: %s", string(pe))
}

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)