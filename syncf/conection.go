package syncf

import (
	"log"
	"net"
	"sync"
	"time"
)

// multi addr connection pool
// TODO only support net.Conn now, can support more kind of Conn interface
type Connection struct {
	Conn   net.Conn
	Addr string
	cpool *ConnPool
	time time.Time
}

type ConnPool struct {
	DiaTimout time.Duration
	RWTimeout time.Duration
	MaxIdleConns int
	lk       sync.Mutex
	freeconn map[string][]*Connection
}

func (cn *Connection) ExtendDeadline() {
	cn.Conn.SetDeadline(time.Now().Add(cn.cpool.RWTimeout))
}

func (c *ConnPool) Put(addr string, cn *Connection, err *error) {
	if *err != nil {
		cn.Conn.Close()
		return
	}

	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[string][]*Connection)
	}
	freelist := c.freeconn[addr]
	if len(freelist) >= c.MaxIdleConns {
		cn.Conn.Close()
		return
	}
	cn.time = time.Now()
	c.freeconn[addr] = append(freelist, cn)
}

func (c *ConnPool) getFreeConn(addr string) (cn *Connection, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *ConnPool) Get(addr string) (*Connection, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		cn.ExtendDeadline()
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &Connection {
		Conn:   nc,
		Addr:  addr,
		cpool:    c,
		time: time.Now(),
	}
	cn.ExtendDeadline()
	return cn, nil
}

func (c *ConnPool) dial(addr string) (net.Conn, error) {
	type connError struct {
		cn  net.Conn
		err error
	}

	nc, err := net.DialTimeout("tcp", addr, c.DiaTimout)
	if err == nil {
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

type ConnectTimeoutError struct {
	addr string
}

func (cte *ConnectTimeoutError) Error() string {
	return "connect timeout " + cte.addr
}


func (c *ConnPool) CheckIdleConn(sec int) {
	timer := time.NewTicker(time.Second * 10)
	var now time.Time
	var dur time.Duration
	for {
		select {
		case <-timer.C:
			c.lk.Lock()
			now = time.Now()
			for addr, _ := range c.freeconn {
				freelist := c.freeconn[addr]
				for i := len(freelist); i > 0; i--{
					dur = now.Sub(freelist[i-1].time)
					if int(dur.Seconds()) > sec {
						log.Println("ConnPool.CheckIdleConn delte *Connection", sec, freelist[i-1].Conn.RemoteAddr())
						freelist[i-1].Conn.Close()
						c.freeconn[addr] = freelist[:i-1]
					}
				}
			}
			c.lk.Unlock()
		}
	}
}
