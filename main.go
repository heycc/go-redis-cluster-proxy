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

	ch := make(chan proxy.Session, 10)
	//ch := make(chan int, 10)
	go handleConnection(ch, server)

	for id := 0; ; id += 1 {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(conn.RemoteAddr())
			ch <- proxy.NewSession(conn)
		}
	}
}

func handleConnection(sessList chan proxy.Session, server proxy.Proxy) {
	for {
		session := <- sessList
		fmt.Println("handleConnection", session)
		go func () {
			session.Exec(server)
		}()
	}
}