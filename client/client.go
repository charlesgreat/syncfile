package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"path/filepath"
	"syncfile/syncf"
	"time"
)

// for test

const (
	ReadWriteDeadLine = 15*time.Second
	CommonFileReadSize = 200*1024*1024 //can compress more than 80%
	DataFileReadSize = 20*1024*1024  // compress only little
)

var (
	clientCfg ClientCfgInfo
)


func main() {
	initEnv()

	go func() {
		log.Println(http.ListenAndServe(clientCfg.DebugAddr, nil))
	}()

	startMonitrFile()

	checkDifWithSvr()

	FileChgHandleLoop()
}

func initEnv() {
	//load cfg

	bRst := syncf.LoadConfig("./Conf/client.conf", &clientCfg)
	if !bRst {
		log.Fatal()
	}

	clientCfg.LRPathMapWithPre = make(map[string]string)
	if clientCfg.GoRPoolSize != 0 {
		lGPoolSize = clientCfg.GoRPoolSize
	}

	for l, r := range clientCfg.LRPathMap {
		path, err := filepath.Abs(l)
		if err != nil {
			continue
		}
		svrPath := clientCfg.RemotePathPre + r
		clientCfg.LRPathMapWithPre[path] = svrPath
		clientCfg.RPathWithPre = append(clientCfg.RPathWithPre, svrPath)
	}
}

