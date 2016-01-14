package proxy

import (
	"log"
	"net"
	"strconv"
)

type proxy interface {
	//initBackend() error
	Close() error
	Do(string) (interface{}, error)
}
type Proxy struct {
	totalSlots int
	slotMap    []string
	addrList   []string
	chanSize   int
	backend    map[string](chan Conn)
	adminConn  Conn
}

var (
	SLOTSIZE = 16384
)

func NewProxy(address string) proxy {
	net, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("failed to dail cluster " + address + " " + err.Error())
	}
	conn := NewConn(net, 10, 10)

	p := &Proxy{
		addrList:  nil,
		chanSize:  2,
		backend:   nil,
		adminConn: conn,
	}

	p.init()

	return p
}

// close connection
func (proxy *Proxy) Close() error {
	// TODO
	return nil
}

// checkState check that cluster is available
func (proxy *Proxy) checkState() error {
	// TODO
	return nil
}

func (proxy *Proxy) init() {
	proxy.totalSlots = SLOTSIZE
	proxy.slotMap = make([]string, proxy.totalSlots)
	proxy.addrList = make([]string, 0)

	// TODO: check redis cluster is READY!
	err := proxy.checkState()
	if err != nil {
		log.Fatal(err)
	}

	proxy.initSlot()
	proxy.initBackend()
}

// initSlot get nodes list and slot distribution
func (proxy *Proxy) initSlot() {
	conn := proxy.adminConn
	reply, err := conn.Do("cluster slots")

	if err == nil {
		if reply, ok := reply.([]interface{}); ok {
			// For each slots range
			for _, slots := range reply {
				if slots, ok := slots.([]interface{}); ok {
					var slot_from, slot_to, slot_port int64
					var slot_addr string
					tmp := slots[0]
					if tmp, ok := tmp.(int64); ok {
						slot_from = tmp
					}
					tmp = slots[1]
					if tmp, ok := tmp.(int64); ok {
						slot_to = tmp
					}
					tmp = slots[2]
					if tmp, ok := tmp.([]interface{}); ok {
						if tmp2, ok := tmp[0].([]uint8); ok {
							slot_addr = string(tmp2)
						}
						if tmp2, ok := tmp[1].(int64); ok {
							slot_port = tmp2
						}
					}

					// add node address to proxy.addrList
					for i := slot_from; i <= slot_to; i++ {
						tmpAddr := slot_addr + ":" + strconv.FormatInt(slot_port, 10)
						proxy.slotMap[i] = tmpAddr

						newAddr := true
						for _, addr := range proxy.addrList {
							if addr == tmpAddr {
								newAddr = false
							}
						}
						if newAddr {
							proxy.addrList = append(proxy.addrList, tmpAddr)
						}
					}
				}
			}
			log.Println("master nodes", proxy.addrList)
		}
	} else {
		log.Fatal("cluster slots error. " + err.Error())
	}
}

// initBackend init connection to all redis nodes
func (proxy *Proxy) initBackend() {
	proxy.backend = make(map[string](chan Conn), len(proxy.addrList))
	for _, addr := range proxy.addrList {
		proxy.backend[addr] = make(chan Conn, proxy.chanSize)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatal("failed to dail node " + addr + " " + err.Error())
		}
		log.Println("init backend connection", addr, proxy.chanSize)
		for i := 0; i < proxy.chanSize; i++ {
			proxy.backend[addr] <- NewConn(conn, 10, 10)
		}
	}
}

func (proxy *Proxy) Do(cmd string) (interface{}, error) {
	defaultAddr := "127.0.0.1:7101"
	conn := <-proxy.backend[defaultAddr]
	conn.writeCmd(cmd)
	reply, err := conn.readReply()
	proxy.backend[defaultAddr] <- conn
	if err != nil {
		// log.Printf("error %s", err.Error())
		switch err := err.(type) {
		case *movedError:
			realAddr := err.Address
			conn = <-proxy.backend[realAddr]
			conn.writeCmd(cmd)
			tmp, err := conn.readReply()
			proxy.backend[realAddr] <- conn
			return tmp, err
		case *askError:
			realAddr := err.Address
			conn = <-proxy.backend[realAddr]
			conn.writeCmd("ASKING")
			_, tmpErr := conn.readReply()
			if tmpErr != nil {
				log.Println("asking failed", tmpErr.Error())
			}
			conn.writeCmd(cmd)
			tmp, err := conn.readReply()
			proxy.backend[realAddr] <- conn
			return tmp, err
		default:
			return reply, err
		}
	} else {
		return reply, err
	}
}
