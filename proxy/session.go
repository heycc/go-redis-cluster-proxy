package proxy

import (
	"time"
)
type session struct {
	ts	time.Time
	conn net.Conn
	ops	int64
	remote	string
}

func handleSession (conn net.Conn) {

}