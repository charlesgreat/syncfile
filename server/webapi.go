package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"syncfile/syncf"
)

func WebAPILoop() {
	http.HandleFunc("/api/getpathfile", GetFiles)
	log.Fatal(http.ListenAndServe(svrCfg.SvrApiAddr, nil))
}

func GetFiles(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("GetFiles, ioutil.ReadAll failed", err)
		return
	}

	var req syncf.PathFileReq
	if err = json.Unmarshal(body, &req); err != nil {
		log.Println("Unmarshal err ", err)
		return
	}

	var rsp syncf.PathFileRsq
	var pathFiles syncf.PathFiles
	var path string
	for _, val := range req.RPaths {
		path = svrCfg.LRPath+val
		pathFiles.Path= path
		pathFiles.Files = syncf.GetPathFileStat(path)
		pathFiles.Path= val  // reset to req path
		rsp.Pathfiles = append(rsp.Pathfiles, pathFiles)
	}

	rsp.Result = 0
	var data []byte
	data, err = json.Marshal(rsp)
	if err != nil {
		log.Println("GetFiles, json.Marshal failed", err)
		return
	}

	_,_ =w.Write(data)

}