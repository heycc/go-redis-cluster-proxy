package main

import (
	"fmt"
	"net"
	"runtime"
	"./proxy"
)
func main () {
	runtime.GOMAXPROCS(4)
	addr := "127.0.0.1:7102"
    server := proxy.NewProxy(addr)

	ln, err := net.Listen("tcp", ":7011")
	if err != nil {
		fmt.Println(err.Error())
	}

	ch := make(chan net.Conn, 10)
	//ch := make(chan int, 10)
	// go handleConnection(ch, server)
	go func() {
		// fmt.Println("11", server)
		// server.GetAddr()
		for conn := range ch {
			// fmt.Println("for range loop", conn.RemoteAddr())
			go func(conn net.Conn) {
				// fmt.Println("22", server)
				// fmt.Println("go in loop", conn.RemoteAddr())
				proxy.NewSession(conn).Work(server)
				// session.Exec(server)
			}(conn)
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept error", err.Error())
		} else {
			// fmt.Println("accept", conn.RemoteAddr())
			ch <- conn
		}
	}
}