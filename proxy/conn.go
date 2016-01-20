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

type Conn interface {
	// execute a redis command
	Do(cmd string) (reply interface{}, err error)
	// get buffered response
	GetResponse() []byte
	// write string command
	writeCmd(string) error
	// get reply from redis server
	readReply() (interface{}, error)
	// clear connection buffer info
	clear() error
}

// NewConn returns a new connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout int64) Conn {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  time.Duration(readTimeout) * time.Millisecond,
		writeTimeout: time.Duration(readTimeout) * time.Millisecond,
		command:      make([]byte, 0),
		response:     make([]byte, 0),
	}
}

type conn struct {
	conn net.Conn
	// Read
	readTimeout time.Duration
	br          *bufio.Reader
	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer

	command  []byte
	response []byte
}

func (c *conn) readLine() ([]byte, error) {
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

func (c *conn) readLen(len int64) ([]byte, error) {
	p := make([]byte, len)
	_, err := io.ReadFull(c.br, p)
	c.bufferResponse(p)
	return p, err
}

func (c *conn) bufferResponse(resp []byte) error {
	c.response = append(c.response, resp...)
	return nil
}

func (c *conn) GetResponse() []byte {
	return c.response
}

func (c *conn) clear() error {
	c.response = nil
	c.command = nil
	return nil
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
			return nil, &movedError{Slot: slot, Address: lineArr[2]}
		// ASK
		case len(lineArr) == 3 && lineArr[0] == "-ASK":
			slot, err := strconv.ParseInt(lineArr[1], 10, 64)
			if err != nil {
				return nil, protocolError("ASK error parse slot failed: " + err.Error())
			}
			return nil, &askError{Slot: slot, Address: lineArr[2]}
		default:
			return nil, Error(string(line[1:]))
		}
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

func (c *conn) writeCmd(cmd string) error {
	cmdArr := regexp.MustCompile(" +").Split(cmd, 99)
	cmdStr := fmt.Sprintf("*%d\r\n", len(cmdArr))
	for _, val := range cmdArr {
		cmdStr += fmt.Sprintf("$%d\r\n", len(val))
		cmdStr += fmt.Sprintf("%s\r\n", val)
	}
	return c.writeBytes([]byte(cmdStr))
}

func (c *conn) writeBytes(cmd []byte) error {
	c.bw.Write(cmd)
	if err := c.bw.Flush(); err != nil {
		return protocolError("flush error")
	}
	return nil
}

func (c *conn) Do(cmd string) (interface{}, error) {
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
