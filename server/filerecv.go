package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"syncfile/syncf"
	"time"
)

type Request struct {
	header      ReqHeader
	data        []byte
}

type ReqHeader struct {
	filePath    string
	sPos        int
	size        int
	tolSize     int
	comprs      bool
}

type ConInfo struct {
	in     []byte
	out    []byte
	Conn   net.Conn
	Req    Request
	Action      int //是否关闭连接
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 20*1024*1024)
	},
}

func putHandlePool(conn net.Conn) (err error) {
	err = grPool.Submit(func() {
		handleNewConn(conn)
	})
	if err != nil {
		log.Println("grPool.Submit failed")
	}
	return err
}

func handleNewConn(conn net.Conn) {
	buf := bufPool.Get().([]byte)
	defer func() {
		bufPool.Put(buf)
		log.Println("Close connection after handleNewConn", conn.RemoteAddr())
		_ = conn.Close()
	}()

	var conInfo ConInfo
	conInfo.Conn = conn
	for {
		nr, err := conn.Read(buf[:cap(buf)])
		if err != nil {
			log.Println(err, conn.RemoteAddr())
			return
		}

		buf = buf[0:nr]
		handleData(&conInfo, buf)
		if conInfo.Action == Close {
			return
		}

	}
}

func handleData(conInfo *ConInfo, buf []byte) {
	req := &conInfo.Req
	data := conInfo.Begin(buf)
	for {
		req.Reset()
		conInfo.out = conInfo.out[0:0]
		conInfo.Action = None
		iRst, leftover := parseReq(data, req)
		if iRst < 0 {
			log.Println("parseReq failed", iRst)
			conInfo.Action = Close
			break
		}

		if iRst == 1 {
			break
		}


		if req.header.comprs {
			req.data = syncf.GzipUnCompress(req.data)
		}

		iPos := 0
		iRst, iPos = handleOperation(conInfo)
		BuildRspData(iRst, iPos, conInfo)

		if len(conInfo.out) > 0 {
			err := conInfo.Conn.SetWriteDeadline(time.Now().Add(time.Second*ReadWriteDeadLine))
			if err != nil {
				log.Println("SetWriteDeadline failed", conInfo.Conn.RemoteAddr(), err)
				conInfo.Action = Close
				return
			}
			_, err = conInfo.Conn.Write(conInfo.out)
			if err != nil {
				log.Println("Write failed", conInfo.Conn.RemoteAddr(), err)
				conInfo.Action = Close
				return
			}
			conInfo.out = conInfo.out[:0]
		}

		data = leftover
		if len(data) == 0 {
			break
		}

	}
	conInfo.End(data)
}

// -1 failed, 0 succeed, 1 not enough
// ture header complete
// path pos size tsize ndata
func parseReq(buf []byte, req *Request) (int, []byte) {
	leftover := buf[0:]
	iHEnd := bytes.IndexByte(buf, '\n')
	if iHEnd < 0 {
		return 1, leftover
	}

	strHead := string(buf[0:iHEnd])
	iRst, err := fmt.Sscanf(strHead, "%s %d %d %d %t", &req.header.filePath,
		&req.header.sPos, &req.header.size, &req.header.tolSize, &req.header.comprs)
	if iRst != 5 || err != nil {
		return -1, leftover
	}

	if req.header.sPos < 0 || req.header.size < 0 || req.header.tolSize < 0 {
		return -1, leftover
	}

	iData := len(buf)-iHEnd-1
	if req.header.size > iData {
		return 1, leftover
	}

	iEnd := iHEnd+1+req.header.size

	if len(buf) > iEnd{
		req.data = append(req.data, buf[iHEnd+1:iEnd]...)
		leftover = buf[iEnd:]
		return 0, leftover
	} else {
		req.data = append(req.data, buf[iHEnd+1:]...)
		return 0, nil
	}

	// speed up
	//leftover := buf[0:]
	//nPath := bytes.IndexByte(buf, ' ')
	//if nPath < 0 {
	//	return 1, leftover
	//}
	//
	//nPos := bytes.IndexByte(buf[nPath+1:], ' ')
	//if nPos < 0 {
	//	return 1, leftover
	//}
	//
	//nSize := bytes.IndexByte(buf[nPos+1:], ' ')
	//if nSize < 0 {
	//	return 1, leftover
	//}
	//
	//nTsize := bytes.IndexByte(buf[nSize+1:], ' ')
	//if nTsize < 0 {
	//	return 1, leftover
	//}
	//
	//nComprs := bytes.IndexByte(buf[nTsize+1:], ' ')
	//if nTsize < 0 {
	//	return 1, leftover
	//}
	//
	//iData := len(buf)-nComprs-1
	//if req.header.size > iData {
	//	return 1, leftover
	//}
	//
	//req.header.filePath = string(buf[:nPath])
	//req.header.sPos, _ = strconv.Atoi(string(buf[nPath+1:nPos]))
	//req.header.size, _ = strconv.Atoi(string(buf[nPos+1:nSize]))
	//req.header.tolSize, _ = strconv.Atoi(string(buf[nSize+1:nTsize]))
	//if i,_ := strconv.Atoi(string(buf[nTsize+1:nComprs])); i > 0 {
	//	req.header.comprs = true
	//}
}


func (conn *ConInfo) Begin(packet []byte) (data []byte) {
	data = packet
	if len(conn.in) > 0 {
		conn.in = append(conn.in, data...)
		data = conn.in
	}
	return data
}

func (conn *ConInfo) End(data []byte) {
	if len(data) > 0 {
		if len(data) != len(conn.in) {
			conn.in = append(conn.in[:0], data...)
		}
	} else if len(conn.in) > 0 {
		conn.in = conn.in[:0]
	}
}

func handleOperation(conInfo *ConInfo) (int, int){
	req := &conInfo.Req
	var fileName string
	var file *os.File
	var err error
	iRst := 0
	nw := 0
	if req.header.filePath[0] != '/' {
		fileName = svrCfg.LRPath + "/" + req.header.filePath
	} else {
		fileName = svrCfg.LRPath + req.header.filePath
	}

	bFileExist := syncf.CheckFileIsExist(fileName)
	if !bFileExist {
		syncf.CreateFilePathF(fileName)
	}

	// new file
	if req.header.sPos == 0 {
		iRst, _ = fileHandleMap.GetFileHandleInfo(fileName)
		if iRst == syncf.FileUsing {
			return syncf.FileUsing, 0
		} else if iRst == syncf.FileInfoCanUse {
			fileHandleMap.RemoveFileHandleInfo(fileName)
		}

		if bFileExist { //delete first
			err := os.Remove(fileName)
			if err != nil {
				log.Println("Remove failed ", fileName, err)
				return syncf.FileRemoveErr, 0
			}
		}

		//create file
		file, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0777)
		if err != nil {
			log.Println("OpenFile failed ", fileName, err)
			return syncf.FieleCreateErr, 0
		}

		//write data
		nw, err = file.Write(req.data)
		if err != nil {
			log.Println("Wirte failed ", fileName, err)
			file.Close()
			return syncf.FileWriteErr, 0
		}

		fileHandleMap.AddFileHandleInfo(fileName, file)
		return syncf.Succeed, nw
	}

	if !bFileExist {
		return syncf.FileNotExist, 0
	}

	// old file
	var fileInfo *syncf.FileHandleInfo
	iRst, fileInfo = fileHandleMap.GetFileHandleInfo(fileName)
	if iRst == syncf.FileInfoUsing {
		return syncf.FileUsing, 0
	}

	if iRst == syncf.FileInfoNo {
		file, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0777)
		if err != nil {
			log.Println("OpenFile failed ", fileName, err)
			return syncf.FieleCreateErr, 0
		}

		iRst, nw = syncf.CheckPosAndWrte(file, req.header.sPos, req.data)
		if iRst != syncf.Succeed {
			log.Println("syncf.CheckPosAndWrte failed reqpos", req.header.sPos, iRst, fileName)
			file.Close()
			return iRst, nw
		}

		fileHandleMap.AddFileHandleInfo(fileName, file)
		return iRst, nw
	}

	file = fileInfo.File
	iRst, nw = syncf.CheckPosAndWrte(file, req.header.sPos, req.data)
	if iRst != syncf.Succeed {
		log.Println("syncf.CheckPosAndWrte failed reqpos", req.header.sPos, iRst, fileName)
		fileHandleMap.RemoveFileHandleInfo(fileName)
		return iRst, nw
	}

	fileHandleMap.PutFileHandleInfo(fileInfo)

	return iRst, 0
}

func (header *ReqHeader) Reset() {
	header.filePath = ""
	header.sPos = 0
	header.size = 0
	header.tolSize = 0
	header.comprs = false
}

func (req *Request) Reset() {
	req.header.Reset()
	req.data = req.data[0:0]
}

func BuildRspData(iRst int, iPos int, conInfo *ConInfo) {
	conInfo.out = append(conInfo.out, syncf.FileOprErr[iRst])
	conInfo.out = append(conInfo.out, ' ')
	conInfo.out = append(conInfo.out, []byte(strconv.Itoa(iPos))...)
	conInfo.out = append(conInfo.out, '\n')
}
