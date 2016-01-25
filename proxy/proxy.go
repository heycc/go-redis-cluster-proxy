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

const (
	SLOTSIZE = 16384
	BACKENSIZE = 5
)

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
		chanSize:   BACKENSIZE,
		backend:    nil,
		adminConn:  conn,
	}
	p.init()
	return p
}

// close connection
func (p *proxy) Close() error {
	// TODO
	return nil
}

// checkState check that cluster is available
func (p *proxy) checkState() error {
	// TODO
	return nil
}

func (p *proxy) init() {
	p.slotMap = make([]string, p.totalSlots)
	p.addrList = make([]string, 0)

	// TODO: check redis cluster is READY!
	err := p.checkState()
	if err != nil {
		log.Fatal(err)
	}

	p.initSlotMap()
	p.initBackend()
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

			tmpAddr := slot_addr + ":" + strconv.FormatInt(slot_port, 10)
			for i := slot_from; i <= slot_to; i++ {
				p.slotMap[i] = tmpAddr
			}

			// add node address to proxy.addrList
			isNew := true
			for _, addr := range p.addrList {
				if addr == tmpAddr {
					isNew = false
					break
				}
			}
			if isNew { p.addrList = append(p.addrList, tmpAddr) }
		}
		log.Println("cluster nodes: ", p.addrList)
	} else { log.Fatal("cluster slots error. " + err.Error()) }
}

// initBackend init connection to all redis nodes
func (p *proxy) initBackend() {
	p.backend = make(map[string](chan Conn), len(p.addrList))
	for _, addr := range p.addrList {
		p.backend[addr] = make(chan Conn, p.chanSize)
		log.Println("init backend connection", addr, p.chanSize)
		for i := 0; i < p.chanSize; i++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatal("failed to dail node " + addr + " " + err.Error())
			}
			p.backend[addr] <- NewConn(conn, 10, 10)
		}
	}
}

func (p *proxy) getConn(addr string) Conn {
	log.Println("free conn", addr, len(p.backend[addr]), cap(p.backend[addr]))
	return <-p.backend[addr]
}

func (p *proxy) returnConn(addr string, conn Conn) error {
	log.Println("return", addr)
	conn.clear()
	p.backend[addr] <- conn
	return nil
}

func (p *proxy) do(cmd []byte) ([]byte, error) {
	id := 0
	return p.doAtSlot(cmd, id)
}

func (p *proxy) doAtSlot(cmd []byte, id int) ([]byte, error) {
	if !(id >= 0 && id < SLOTSIZE) {
		return nil, Error("slot id out of range: " + string(id))
	}

	theAddr := p.slotMap[id]
	conn := p.getConn(theAddr)
	conn.writeBytes(cmd)
	_, err := conn.readReply()
	resp := conn.getResponse()
	p.returnConn(theAddr, conn)

	if err != nil {
		switch err := err.(type) {
		case *movedError:
			// get MOVED error for the first time, follow new address
			theAddr = err.Address
			conn = p.getConn(theAddr)
			conn.writeBytes(cmd)
			_, err2 := conn.readReply()
			resp = conn.getResponse()
			p.returnConn(theAddr, conn)

			switch err2 := err2.(type) {
			case *askError:
				// ASK error after MOVED error, follow new address
				theAddr = err2.Address
				conn = p.getConn(theAddr)
				conn.writeCmd("ASKING")
				if _, err3 := conn.readReply(); err3 != nil {
					p.returnConn(theAddr, conn)
					return nil, Error("ASKING failed " + err3.Error())
				}
				conn.clear()
				conn.writeBytes(cmd)
				_, err3 := conn.readReply()
				resp = conn.getResponse()
				p.returnConn(theAddr, conn)

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
			conn = p.getConn(theAddr)
			conn.writeCmd("ASKING")
			if _, err3 := conn.readReply(); err3 != nil {
				p.returnConn(theAddr, conn)
				return nil, Error("ASKING failed " + err3.Error())
			}
			conn.clear()
			conn.writeBytes(cmd)
			_, err3 := conn.readReply()
			resp = conn.getResponse()
			p.returnConn(theAddr, conn)
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
