package proxy

import (
	"log"
	"net"
	// "fmt"
	"strconv"
)

const (
	SLOTSIZE   = 16384
	BACKENSIZE = 4
)

type Proxy interface {
	Close() error
	do([]byte) ([]byte, error)
	slotDo([]byte, uint16) ([]byte, error)
	GetAddr()
}

type proxy struct {
	totalSlots int
	slotMap    []string
	addrList   []string
	chanSize   int
	backend    map[string](chan RedisConn)
	adminConn  RedisConn
}

func NewProxy(address string) Proxy {
	net, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("failed to dail cluster " + address + " " + err.Error())
	}
	p := &proxy{
		totalSlots: SLOTSIZE,
		slotMap:    nil,
		addrList:   nil,
		chanSize:   BACKENSIZE,
		backend:    nil,
		adminConn:  NewConn(net, 10, 10),
	}
	p.init()
	return p
}

func (p *proxy) GetAddr() {
	log.Println(p.adminConn)
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

	if err != nil {
		log.Fatal("cluster slots error. " + err.Error())
		return
	}
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
		if isNew {
			p.addrList = append(p.addrList, tmpAddr)
		}
	}
	log.Println("cluster nodes:", p.addrList)
}

// initBackend init connection to all redis nodes
func (p *proxy) initBackend() {
	p.backend = make(map[string](chan RedisConn), len(p.addrList))
	for _, addr := range p.addrList {
		p.backend[addr] = make(chan RedisConn, p.chanSize)
		log.Println("init backend connection to", addr, ", pool size", p.chanSize)
		for i := 0; i < p.chanSize; i++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatal("failed to dail node " + addr + " " + err.Error())
			}
			c := NewConn(conn, 10, 10)
			p.backend[addr] <- c
		}
	}
}

func (p *proxy) exec(cmd []byte, addr string) ([]byte, error) {
	conn := <-p.backend[addr]
	conn.writeBytes(cmd)
	_, err := conn.readReply()
	resp := conn.getResponse()
	conn.clear()
	p.backend[addr] <- conn
	return resp, err
}

func (p *proxy) execWithAsk(cmd []byte, addr string) ([]byte, error) {
	conn := <-p.backend[addr]
	conn.writeCmd("ASKING")
	if _, err := conn.readReply(); err != nil {
		conn.clear()
		p.backend[addr] <- conn
		return nil, protocolError("ASKING failed " + err.Error())
	}
	conn.clear()
	conn.writeBytes(cmd)
	_, err := conn.readReply()
	resp := conn.getResponse()
	conn.clear()
	p.backend[addr] <- conn
	return resp, err
}

func (p *proxy) do(cmd []byte) ([]byte, error) {
	return p.slotDo(cmd, 0)
}

func (p *proxy) slotDo(cmd []byte, id uint16) ([]byte, error) {
	if !(id >= 0 && id < SLOTSIZE) {
		return nil, protocolError("slot id out of range: " + string(id))
	}
	resp, err := p.exec(cmd, p.slotMap[id])
	if err == nil {
		return resp, nil
	}
	switch errVal := err.(type) {
	case movedError:
		// get MOVED error for the first time, follow new address, update slot mapping
		resp, err := p.exec(cmd, errVal.Address)
		switch errVal := err.(type) {
		case askError:
			// ASK error after MOVED error, follow new address
			return p.execWithAsk(cmd, errVal.Address)
		case movedError:
			// MOVED error after MOVED error, this shouldn't happen
			return nil, protocolError("Error! MOVED after MOVED")
		default:
			return resp, errVal
		}
	case askError:
		// get ASK error for the first time, follow new address
		return p.execWithAsk(cmd, errVal.Address)
	default:
		return resp, errVal
	}
}
