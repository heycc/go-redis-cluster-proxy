package proxy

import (
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Proxy interface {
	Close() error
	do([]byte) ([]byte, error)
	slotDo([]byte, uint16) ([]byte, error)
	GetAddr()
}

type proxy struct {
	totalSlots   int
	slotMap      []string
	addrList     []string
	chanSize     int
	backend      map[string](chan RedisConn)
	adminConn    RedisConn
	slotMapMutex sync.RWMutex
	backendLock  sync.Mutex
}

func NewProxy(address string) Proxy {
	net, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatal("failed to dail cluster " + address + " " + err.Error())
	}
	p := &proxy{
		totalSlots:   SLOTSIZE,
		slotMap:      nil,
		addrList:     nil,
		chanSize:     BACKENSIZE,
		backend:      nil,
		adminConn:    NewConn(net, 10, 10),
		slotMapMutex: sync.RWMutex{},
		backendLock:  sync.Mutex{},
	}
	p.init()
	return p
}

func (p *proxy) GetAddr() {
	log.Println(p.adminConn)
}

// close connection
func (p *proxy) Close() error {
	log.Println("closing backend connection")
	for _, addr := range p.addrList {
		for conn := range p.backend[addr] {
			conn.close()
		}
	}
	return nil
}

// checkState check that cluster is available
func (p *proxy) checkState() error {
	p.adminConn.writeCmd("CLUSTER INFO")
	reply, err := p.adminConn.readReply()
	if err != nil {
		return err
	}
	reply_body := reply.([]uint8)
	for _, line := range strings.Split(string(reply_body), "\r\n") {
		line_arr := strings.Split(line, ":")
		if line_arr[0] == "cluster_state" {
			if line_arr[1] != "ok" {
				return protocolError(string(reply_body))
			} else {
				return nil
			}
		}
	}
	return protocolError("checkState should never run up to here")
}

func (p *proxy) init() {
	p.slotMap = make([]string, p.totalSlots)
	p.addrList = make([]string, 0)
	p.backend = make(map[string](chan RedisConn))
	// TODO: check redis cluster is READY!
	err := p.checkState()
	if err != nil {
		log.Fatal(err)
	}
	p.initSlotMap()
	go p.keepalive()
}

// initSlotMap get nodes list and slot distribution
func (p *proxy) initSlotMap() {
	p.adminConn.writeCmd("CLUSTER SLOTS")
	reply, err := p.adminConn.readReply()
	if err != nil {
		log.Fatal("cluster slots error. " + err.Error())
		return
	}

	p.addrList = make([]string, 0)

	addrDone := make(map[string]bool)

	for _, slots := range reply.([]interface{}) {
		slotsData := slots.([]interface{})
		slot_from := slotsData[0].(int64)
		slot_to := slotsData[1].(int64)

		addr_tmp := slotsData[2].([]interface{})
		slot_addr := string(addr_tmp[0].([]uint8))
		slot_port := addr_tmp[1].(int64)
		tmpAddr := slot_addr + ":" + strconv.FormatInt(slot_port, 10)

		// add node address to proxy.addrList
		isNew := true
		for _, addr := range p.addrList {
			if tmpAddr == addr {
				isNew = false
				break
			}
		}
		if isNew {
			p.addrList = append(p.addrList, tmpAddr)
		}

		for i := slot_from; i <= slot_to; i++ {
			if p.slotMap[i] != tmpAddr {
				if _, ok := addrDone[tmpAddr]; !ok {
					p.initBackendByAddr(tmpAddr)
					addrDone[tmpAddr] = true
				}
				p.slotMapMutex.Lock()
				p.slotMap[i] = tmpAddr
				p.slotMapMutex.Unlock()
			}
		}
	}
	log.Println("cluster nodes:", p.addrList)
}

// If force is true, discard current connection regardless of dead or alive,
// replace it with new connection.
// Use is when connection is broken.
func (p *proxy) initBackendByAddr(addr string) {
	if _, ok := p.backend[addr]; !ok {
		log.Println("init backend connection to", addr, ", pool size", p.chanSize)
		p.backend[addr] = make(chan RedisConn, p.chanSize)
		for i := 0; i < p.chanSize; i++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatal("failed to dail node " + addr + " " + err.Error())
			}
			c := NewConn(conn, 10, 10)
			p.backend[addr] <- c
		}
	} else {
		for i := 0; i < p.chanSize; i++ {
			conn := <-p.backend[addr]
			if err := conn.ping(); err == nil {
				p.backend[addr] <- conn
				break
			}
			log.Println("connection to ", addr, " failed, replace with new one")
			conn1, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatal("failed to dail node " + addr + " " + err.Error())
			}
			c := NewConn(conn1, 10, 10)
			p.backend[addr] <- c
		}
	}
}

func (p *proxy) keepalive() {
	for true {
		time.Sleep(5 * time.Second)
		for _, addr := range p.addrList {
			p.initBackendByAddr(addr)
		}
	}
}

func (p *proxy) exec(cmd []byte, addr string, ask bool) ([]byte, error) {
	// TODO: check addr in p.backend
	if _, ok := p.backend[addr]; !ok {
		p.backendLock.Lock()
		if _, ok := p.backend[addr]; !ok {
			p.initBackendByAddr(addr)
		}
		p.backendLock.Unlock()
	}

	conn := <-p.backend[addr]

	if ask {
		conn.writeCmd("ASKING")
		if _, err := conn.readReply(); err != nil {
			conn.clear()
			p.backend[addr] <- conn
			return nil, protocolError("ASKING failed " + err.Error())
		}
		conn.clear()
	}

	conn.writeBytes(cmd)
	_, err := conn.readReply()
	resp := conn.getResponse()
	conn.clear()
	p.backend[addr] <- conn
	return resp, err
}

func (p *proxy) execNoAsk(cmd []byte, addr string) ([]byte, error) {
	return p.exec(cmd, addr, false)
}

func (p *proxy) execWithAsk(cmd []byte, addr string) ([]byte, error) {
	return p.exec(cmd, addr, true)
}

func (p *proxy) do(cmd []byte) ([]byte, error) {
	return p.slotDo(cmd, 0)
}

func (p *proxy) slotDo(cmd []byte, id uint16) ([]byte, error) {
	if !(id >= 0 && id < SLOTSIZE) {
		return nil, protocolError("slot id out of range: " + string(id))
	}

	p.slotMapMutex.RLock()
	addr := p.slotMap[id]
	p.slotMapMutex.RUnlock()

	resp, err := p.execNoAsk(cmd, addr)
	if err == nil {
		return resp, nil
	}

	switch errVal := err.(type) {
	case *movedError:
		// get MOVED error for the first time, follow new address, update slot mapping
		resp, err := p.execNoAsk(cmd, errVal.Address)
		switch errVal := err.(type) {
		case *askError:
			// ASK error after MOVED error, follow new address
			return p.execWithAsk(cmd, errVal.Address)
		case *movedError:
			// MOVED error after MOVED error, this shouldn't happen
			return nil, protocolError("Error! MOVED after MOVED")
		default:
			return resp, errVal
		}
	case *askError:
		// get ASK error for the first time, follow new address
		return p.execWithAsk(cmd, errVal.Address)
	default:
		return resp, errVal
	}
}

const (
	SLOTSIZE   = 16384
	BACKENSIZE = 4
)
