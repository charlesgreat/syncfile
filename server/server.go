package main

import (
	"github.com/panjf2000/ants/v2"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syncfile/syncf"
	"time"
)

const (
	ReadWriteDeadLine = 15
)

const (
	None     int = iota
	Close
)

var (
	fileHandleMap syncf.FileHandleMap
	grPool *ants.Pool

	svrCfg SVRCFG

	grPoolSize = 10000
	FileHandleTimeout = 120
)

type SVRCFG struct {
	LRPath            string `json:"LocalRelativePath"`
	SvrUploadAddr     string `json:"SvrUploadAddr"`
	SvrApiAddr        string `json:"SvrApiAddr"`
	DebugAddr         string `json:"DebugAddr"`
	GrPoolSize        int    `json:"GoRoutinePoolSize"`
	FileHandleTimeout int    `json:"FileHandleTimeout"`
}

func main() {
	if  !loadCfg("./Conf/server.conf") {
		log.Fatal()
	}

	go func() {
		log.Println(http.ListenAndServe(svrCfg.DebugAddr, nil))
	}()

	go WebAPILoop()

	fileHandleMap.Map = make(map[string]*syncf.FileHandleInfo, 20)
	go fileHandleMap.CheckUnUsedFileHandle(FileHandleTimeout)

	var err error
	grPool, err = ants.NewPool(grPoolSize)
	if err != nil {
		log.Fatal(err)
	}
	defer grPool.Release()

	if listener, err := net.Listen("tcp", svrCfg.SvrUploadAddr); err == nil {
		// spin-up the client
		log.Println("Server started ", listener.Addr().String())
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal(err)
			}

			log.Println("New connection ", conn.RemoteAddr())
			err = putHandlePool(conn)
			if err != nil {
				log.Println("Close connection ", conn.RemoteAddr(), err)
				conn.Close()
				time.Sleep(time.Second*2)
			}
		}
	} else {
		log.Fatal(err)
	}

}

func loadCfg(cname string) bool {

	bRst := syncf.LoadConfig(cname, &svrCfg)
	if !bRst {
		return false
	}

	if svrCfg.FileHandleTimeout != 0 {
		FileHandleTimeout = svrCfg.FileHandleTimeout
	}

	if svrCfg.GrPoolSize != 0 {
		grPoolSize = svrCfg.GrPoolSize
	}

	err := os.MkdirAll(svrCfg.LRPath, os.ModePerm)
	if err != nil {
		log.Println("os.MkdirAll failed", svrCfg.LRPath, err)
		return false
	}

	return true
}
