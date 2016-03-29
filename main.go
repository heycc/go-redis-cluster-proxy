package main

import (
	"./proxy"
	"./dashboard"
	"fmt"
	"net"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(4)

	//proxyAddr := "127.0.0.1:7101"
	//startProxy(proxyAddr)

	dashboardAddr := "127.0.0.1:7102"
	startDashboard(dashboardAddr)
}

func startProxy(addr string) {
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

func startDashboard(addr string) {
	dashboard := dashboard.NewDashboard(addr)
	dashboard.Start()
}