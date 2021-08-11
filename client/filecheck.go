package main

import (
	"bytes"
	"encoding/json"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"syncfile/syncf"
	"time"
)


const (
	NeedUpload int = iota
	Uploading
	NoneedUpload
)

const (
	FileCommon int = iota
	FileData
)

var (
	DataFileSuffix = []string{".zip", ".gz", ".pdf", ".jpg", ".png",  ".dmg",}
)

type ClientCfgInfo struct {
	RemoteAddr    string  `json:"RemoteAddr"`
	RemoteApiAddr string  `json:"RemoteApiAddr"`
	DebugAddr     string  `json:"DebugAddr"`
	GoRPoolSize   int     `json:"GoRoutinePoolSize"`
	RemotePathPre string `json:"RemotePathPre"`
	LRPathMap   map[string]string  `json:"LocalRemotePathPair"`
	LRPathMapWithPre  map[string]string  // no prefix in conf file, need add to mem cfg
	RPathWithPre []string  // all server paths with prefix
}

type FileEvent struct {
	Fname  string
	Op     int
}

type FileUpInfo struct {
	fname  string
	size     int
	pos      int
	uploading bool
}

type LocalFileMap struct {
	sync.Mutex
	Map map[string]*FileUpInfo
}

type FileChangeMap struct {
	sync.Mutex
	cond *sync.Cond
	Map map[string]struct{}
}

var (
	fileWatcher *fsnotify.Watcher
	//fileEventChan chan fsnotify.Event
	lFileMap LocalFileMap
	fileChangeMap  FileChangeMap
)


func (cfg *ClientCfgInfo) GetSvrFullPath(fname string) string{
	index := strings.LastIndex(fname, "/")
	if index < 1 {
		return ""
	}
	path := fname[:index]

	for k, v := range cfg.LRPathMapWithPre {
		if k == path {
			return v + fname[index:]
		}
	}

	return ""
}

func (fileChangeMap *FileChangeMap) AddFile(fname string) {
	fileChangeMap.Lock()
	defer fileChangeMap.Unlock()

	if _, isExist := fileChangeMap.Map[fname]; !isExist {
		fileChangeMap.Map[fname] = struct{}{}
		fileChangeMap.cond.Signal()
	}
}

func (fileChangeMap *FileChangeMap) DelFile(fname string) {
	fileChangeMap.Lock()
	defer fileChangeMap.Unlock()

	if _, isExist := fileChangeMap.Map[fname]; isExist {
		delete(fileChangeMap.Map, fname)
	}
}

func (fileChangeMap *FileChangeMap) GetAnyFileAndDel() string {
	fileChangeMap.Lock()
	defer fileChangeMap.Unlock()

	if len(fileChangeMap.Map) == 0{
		fileChangeMap.cond.Wait()
	}

	for fname := range fileChangeMap.Map {
		delete(fileChangeMap.Map, fname)
		return fname
	}

	return ""

}

func (localFileMap *LocalFileMap) UpdateFile(fileUpInfo *FileUpInfo) {
	localFileMap.Lock()
	defer localFileMap.Unlock()

	if _, isExist := localFileMap.Map[fileUpInfo.fname]; !isExist {
		localFileMap.Map[fileUpInfo.fname] = fileUpInfo
	} else {
		localFileMap.Map[fileUpInfo.fname].size = fileUpInfo.size
		localFileMap.Map[fileUpInfo.fname].pos = fileUpInfo.pos
		localFileMap.Map[fileUpInfo.fname].uploading = fileUpInfo.uploading
	}
}

func (localFileMap *LocalFileMap) UpdateFileP(fname string, pos int, size int, uploading bool) {
	localFileMap.Lock()
	defer localFileMap.Unlock()

	if _, isExist := localFileMap.Map[fname]; isExist {
		localFileMap.Map[fname].size = size
		localFileMap.Map[fname].pos = pos
		localFileMap.Map[fname].uploading = uploading
	}
}

func (localFileMap *LocalFileMap) SetFileUploading(fname string, uploading bool) {
	localFileMap.Lock()
	defer localFileMap.Unlock()

	if _, isExist := localFileMap.Map[fname]; isExist {
		localFileMap.Map[fname].uploading = uploading
	}
}

func (localFileMap *LocalFileMap) GetAndSetUploadStat(fname string, isize int) (pos int, stat int) {
	localFileMap.Lock()
	defer localFileMap.Unlock()

	if _, isExist := localFileMap.Map[fname]; !isExist {
		fileUpInfo := &FileUpInfo{fname, isize, 0, true}
		localFileMap.Map[fname] = fileUpInfo
		return NeedUpload, 0
	}

	if localFileMap.Map[fname].uploading {
		return Uploading, 0
	}

	if localFileMap.Map[fname].size == isize {
		if localFileMap.Map[fname].pos == localFileMap.Map[fname].size {
			return NoneedUpload, 0
		} else if localFileMap.Map[fname].pos < localFileMap.Map[fname].size {
			localFileMap.Map[fname].uploading = true
			return NeedUpload, localFileMap.Map[fname].pos
		} else {
			log.Println("GetAndSetUploadStat pos > size", fname, localFileMap.Map[fname].pos,isize)
			localFileMap.Map[fname].pos = 0
			localFileMap.Map[fname].uploading = true
			return NeedUpload, 0
		}
	}


	if localFileMap.Map[fname].size > isize {
		log.Println("GetAndSetUploadStat size > newsize, upload again", fname, localFileMap.Map[fname].size, isize)
		localFileMap.Map[fname].pos = 0
		localFileMap.Map[fname].uploading = true
		return NeedUpload, 0
	} else {
		localFileMap.Map[fname].uploading = true
		return NeedUpload, localFileMap.Map[fname].pos
	}

}

func (localFileMap *LocalFileMap) DelFile(fname string) {
	localFileMap.Lock()
	defer localFileMap.Unlock()

	if _, isExist := localFileMap.Map[fname]; isExist {
		delete(localFileMap.Map, fname)
	}
}

func startMonitrFile() {
	var err error
	fileWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("fsnotify.NewWatcher failed: ", err)
	}

	lFileMap.Map =  make(map[string]*FileUpInfo)
	fileChangeMap.Map = make(map[string]struct{})
	fileChangeMap.cond = sync.NewCond(&fileChangeMap.Mutex)

	//fileEventChan = make(chan fsnotify.Event, FileEventChanSize)
	go func() {
		for {
			select {
			case event, ok := <-fileWatcher.Events:
				if !ok {
					return
				}
				if  event.Op&fsnotify.Chmod == fsnotify.Chmod {
					continue
				}

				if syncf.IsDir(event.Name) {
					continue
				}

				//no need check tmp file
				iPos := strings.LastIndex(event.Name, "/")
				if iPos > 0 && iPos < len(event.Name) && event.Name[iPos+1] == '.'{
					continue
				}

				iSpace := strings.Index(event.Name, " ")
				if iSpace >=0 {
					continue
				}

				//fileEventChan <- event
				fileChangeMap.AddFile(event.Name)

			case err, ok := <-fileWatcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	for k, _ := range clientCfg.LRPathMapWithPre {
		err = fileWatcher.Add(k)
		if err != nil {
			log.Fatal("fileWatcher.Add failed:", err)
		}
	}
}

func checkDifWithSvr() {
	var rspPathFile syncf.PathFileRsq
	getFileDesFromSvr(&rspPathFile)

	// get local files
	var lfiles, sfiles []syncf.FileStat
	var fileUpInfo *FileUpInfo
	for kl, vl := range clientCfg.LRPathMapWithPre {
		lfiles = syncf.GetPathFileStat(kl)
		if len(lfiles) == 0 {
			continue
		}
		for _, vs := range rspPathFile.Pathfiles {
			if vs.Path == vl {
				sfiles = vs.Files
				break
			}
		}

		for _, vlf := range lfiles {
			bDif := true
			pos := 0
			for _, vsf := range sfiles {
				if vsf == vlf && vsf.Size == vlf.Size {
					bDif = false
					pos = vsf.Size
				}
			}

			fileUpInfo = &FileUpInfo{kl + "/" + vlf.FileName, vlf.Size, pos, false}
			lFileMap.UpdateFile(fileUpInfo)

			if bDif {
				fileChangeMap.AddFile(fileUpInfo.fname)
			}
		}
	}

}

func getFileDesFromSvr(rspPathFile *syncf.PathFileRsq) {
	var err error
	var resp *http.Response
	var reqData syncf.PathFileReq
	var data []byte
	var body bytes.Buffer
	reqData.RPaths = clientCfg.RPathWithPre
	data, err = json.Marshal(reqData)
	if err != nil {
		log.Fatal("getDiffFromSvr, json.Marshal failed", err)
		return
	}
	body.Write(data)

	client := &http.Client{}
	client.Timeout = time.Second * 15
	url := "http://" + clientCfg.RemoteApiAddr + "/api/getpathfile"
	req, _ := http.NewRequest("Get", url, &body)

	for {
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		log.Println("getDiffFromSvr:" + url + " error:" + err.Error())
		time.Sleep(time.Second*10)
	}

	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("getDiffFromSvr:" + url + "ioutil.ReadAll error:" + err.Error())
		return
	}

	if err = json.Unmarshal(data, rspPathFile); err != nil {
		log.Fatal("getDiffFromSvr Unmarshal err ", err)
		return
	}
}

func CheckFileType(fname string) int {
	for i := 0; i < len(DataFileSuffix); i++ {
		if strings.LastIndex(fname, DataFileSuffix[i]) > 0 {
			return FileData
		}
	}

	return FileCommon
}