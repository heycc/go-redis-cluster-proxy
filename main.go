package main

import (
	"fmt"
	"net"
	"./proxy"
)
func main () {
	addr := "127.0.0.1:7102"
    server := proxy.NewProxy(addr)

	ln, err := net.Listen("tcp", ":7011")
	if err != nil {
		fmt.Println(err.Error())
	}

	ch := make(chan net.Conn, 10)
	//ch := make(chan int, 10)
	go handleConnection(ch, server)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(conn.RemoteAddr())
			ch <- conn
		}
	}
}

func handleConnection(connList chan net.Conn, server proxy.Proxy) {
	for conn := range connList {
		go func () {
			fmt.Println("handleConnection", conn)
			session := proxy.NewSession(conn)
			session.Exec(server)
		}()
	}
}