package main

import (
	"./proxy"
	"fmt"
	"net"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(4)

	addr := "127.0.0.1:7101"
	server := proxy.NewProxy(addr)

	ln, err := net.Listen("tcp", ":7011")
	if err != nil {
		fmt.Println(err.Error())
	}

	ch := make(chan net.Conn, 10)
	go func() {
		for conn := range ch {
			go func(conn net.Conn) {
				proxy.NewSession(conn).Loop(server)
			}(conn)
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept error", err.Error())
		} else {
			ch <- conn
		}
	}
}
