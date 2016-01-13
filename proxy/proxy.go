package proxy

import (
	"net"
	"strconv"
)

type Proxy struct {
	addrList   []string
	chanSize   int
	backend    map[string](chan conn)
	singleConn conn
}

func NewProxy(address string) (Proxy, error) {
	chanSize := 2
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, Error("failed to dail cluster " + err)
	}

	addrList := getAddressList(connection)

	p := &Proxy{
		addrList:   addrList,
		chanSize:   chanSize,
		backend:    nil,
		singleConn: connection,
	}
}

func getAddressList(co conn) []string {
	reply, err := co.Do("cluster slots")
	if err == nil {
		if reply, ok := reply.([]interface{}); ok {
			// For each slots range
			for slots := range reply {
				if slots, ok := slots.([]interface{}); ok {
					slot_from, err := strconv.ParseInt(slots[0], 10, 64)
					slot_to, err := strconv.ParseInt(slots[0], 10, 64)
					if addr, ok := slots[2].([]interface{}); ok {
						slot_addr := string(addr[0])
						slot_port, err := strconv.ParseInt(addr[1])
						fmt.Println(slot_from, slot_to, slot_addr, slot_port)
					}
				}
			}
		}
	}
}

func (proxy *Proxy) createBackend() error {

}
