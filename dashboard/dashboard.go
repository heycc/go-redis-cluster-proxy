package dashboard

import (
	"../proxy"
	//"log"
)

type Dashboard interface {
	Start()
	Stop()
	apiMeet(string) error
	apiSlots() (interface{}, error)
	apiAddSlots(string) error
	apiCountKeysInSlot(uint16) (uint32, error)
	apiGetKeysInSlot(uint16) ([]string, error)
	apiSetSlot(string) error
	apiMigrate(string) error
}

func NewDashboard(addr string) Dashboard {
	dash := &dashboard{
		addrList:    make([]string, 0),
		backendConn: make(map[string]proxy.RedisConn),
	}
	dash.addrList = append(dash.addrList, addr)
	return dash
}

type dashboard struct {
	addrList    []string
	backendConn map[string]proxy.RedisConn
}

func (d *dashboard) Start() {
	return
}

func (d *dashboard) Stop() {
	return
}

func (d *dashboard) apiMeet(newAddr string) error {
	return nil
}

func (d *dashboard) apiSlots() (interface{}, error) {
	return nil, nil
}

func (d *dashboard) apiAddSlots(val string) error {
	return nil
}

func (d *dashboard) apiCountKeysInSlot(id uint16) (uint32, error) {
	return 0, nil
}

func (d *dashboard) apiGetKeysInSlot(id uint16) ([]string, error) {
	return nil, nil
}

func (d *dashboard) apiSetSlot(val string) error {
	return nil
}

func (d *dashboard) apiMigrate(val string) error {
	return nil
}
