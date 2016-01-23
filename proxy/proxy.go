package proxy

import (
	"log"
	"net"
	"strconv"
)

type Proxy interface {
	//initBackend() error
	Close() error
	do([]byte) ([]byte, error)
	doAtSlot([]byte, int) ([]byte, error)
}

type proxy struct {
	totalSlots int
	slotMap    []string
	addrList   []string
	chanSize   int
	backend    map[string](chan Conn)
	adminConn  Conn
}

const SLOTSIZE = 16384

func NewProxy(address string) Proxy {
	net, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("failed to dail cluster " + address + " " + err.Error())
	}
	conn := NewConn(net, 10, 10)
	p := &proxy{
		totalSlots: SLOTSIZE,
		slotMap:    nil,
		addrList:   nil,
		chanSize:   2,
		backend:    nil,
		adminConn:  conn,
	}
	p.init()
	return p
}

// close connection
func (proxy *proxy) Close() error {
	// TODO
	return nil
}

// checkState check that cluster is available
func (proxy *proxy) checkState() error {
	// TODO
	return nil
}

func (proxy *proxy) init() {
	proxy.slotMap = make([]string, proxy.totalSlots)
	proxy.addrList = make([]string, 0)

	// TODO: check redis cluster is READY!
	err := proxy.checkState()
	if err != nil {
		log.Fatal(err)
	}

	proxy.initSlotMap()
	proxy.initBackend()
}

// initSlotMap get nodes list and slot distribution
func (p *proxy) initSlotMap() {
	p.adminConn.writeCmd("CLUSTER SLOTS")
	reply, err := p.adminConn.readReply()

	if err == nil {
		for _, slots := range reply.([]interface{}) {
			slotsData := slots.([]interface{})

			slot_from := slotsData[0].(int64)
			slot_to := slotsData[1].(int64)

			addr_tmp := slotsData[2].([]interface{})
			slot_addr := string(addr_tmp[0].([]uint8))
			slot_port := addr_tmp[1].(int64)

			// add node address to proxy.addrList
			for i := slot_from; i <= slot_to; i++ {
				tmpAddr := slot_addr + ":" + strconv.FormatInt(slot_port, 10)
				p.slotMap[i] = tmpAddr

				isNew := true
				for _, addr := range p.addrList {
					if addr == tmpAddr {
						isNew = false
					}
				}
				if isNew {
					p.addrList = append(p.addrList, tmpAddr)
				}
			}
		}
	} else {
		log.Fatal("cluster slots error. " + err.Error())
	}
}

// initBackend init connection to all redis nodes
func (proxy *proxy) initBackend() {
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

func (proxy *proxy) getConn(addr string) Conn {
	return <-proxy.backend[addr]
}

func (proxy *proxy) returnConn(addr string, conn Conn) error {
	conn.clear()
	proxy.backend[addr] <- conn
	return nil
}

func (proxy *proxy) do(cmd []byte) ([]byte, error) {
	id := 0
	return proxy.doAtSlot(cmd, id)
}

func (proxy *proxy) doAtSlot(cmd []byte, id int) ([]byte, error) {
	if !(id >= 0 && id < SLOTSIZE) {
		return nil, Error("slot id out of range: " + string(id))
	}

	theAddr := proxy.slotMap[id]
	conn := proxy.getConn(theAddr)
	conn.writeBytes(cmd)
	_, err := conn.readReply()
	resp := conn.getResponse()
	proxy.returnConn(theAddr, conn)

	if err != nil {
		switch err := err.(type) {
		case *movedError:
			// get MOVED error for the first time, follow new address
			theAddr = err.Address
			conn = proxy.getConn(theAddr)
			conn.writeBytes(cmd)
			_, err2 := conn.readReply()
			resp = conn.getResponse()
			proxy.returnConn(theAddr, conn)

			switch err2 := err2.(type) {
			case *askError:
				// ASK error after MOVED error, follow new address
				theAddr = err2.Address
				conn = proxy.getConn(theAddr)
				conn.writeCmd("ASKING")
				if _, err3 := conn.readReply(); err3 != nil {
					proxy.returnConn(theAddr, conn)
					return nil, Error("ASKING failed " + err3.Error())
				}
				conn.clear()
				conn.writeBytes(cmd)
				_, err3 := conn.readReply()
				resp = conn.getResponse()
				proxy.returnConn(theAddr, conn)

				if err3 != nil {
					return nil, err3
				} else {
					return resp, nil
				}

			case *movedError:
				// MOVED error after MOVED error, this shouldn't happen
				return nil, Error("Error! MOVED after MOVED")
			default:
				return resp, err2
			}

		case *askError:
			// get ASK error for the first time, follow new address
			theAddr := err.Address
			conn = proxy.getConn(theAddr)
			conn.writeCmd("ASKING")
			if _, err3 := conn.readReply(); err3 != nil {
				proxy.returnConn(theAddr, conn)
				return nil, Error("ASKING failed " + err3.Error())
			}
			conn.clear()
			conn.writeBytes(cmd)
			_, err3 := conn.readReply()
			resp = conn.getResponse()
			proxy.returnConn(theAddr, conn)
			if err3 != nil {
				return nil, err3
			} else {
				return resp, nil
			}

		default:
			return resp, err
		}
	} else {
		return resp, nil
	}
}
