package main

import (
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"log"
	"os"
	"sync"
	"syncfile/syncf"
	"time"
)

var (
	connPool *syncf.ConnPool
	lGPoolSize = 50
)

var (
	lGPool *ants.Pool
	cBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, CommonFileReadSize)
		},
	}
)


func FileChgHandleLoop() {
	var err error
	var fname string
	var fileStat syncf.FileStat
	var upStat int
	var upPos int

	lGPool, err = ants.NewPool(lGPoolSize)
	if err != nil {
		log.Fatal(err)
	}
	defer lGPool.Release()

	connPool = &syncf.ConnPool{DiaTimout:ReadWriteDeadLine,
		RWTimeout:ReadWriteDeadLine, MaxIdleConns:20}
	go connPool.CheckIdleConn(300)

	for {
		fname = fileChangeMap.GetAnyFileAndDel()
		if len(fname) == 0 {
			time.Sleep(time.Second * 10) //abnormal
			continue
		}

		// check pos and uploading
		fileStat, err = syncf.GetFileStat(fname)
		if err != nil {
			log.Println("syncf.GetFileStat failed", err)
			if os.IsNotExist(err) {
				lFileMap.DelFile(fname)
			}
			continue
		}

		upStat,upPos = lFileMap.GetAndSetUploadStat(fname, fileStat.Size)
		if upStat == NoneedUpload {
			continue
		}
		if upStat == Uploading {
			fileChangeMap.AddFile(fname)
			continue
		}

		// upload file
		err = putHandlePool(fname, upPos)
		if err != nil {
			time.Sleep(time.Second*2)
		}
	}
}

func putHandlePool(fname string, pos int) (err error) {
	err = lGPool.Submit(func() {
		handUpload(fname, pos)
	})
	if err != nil {
		log.Println("lGPool.Submit failed")
	}
	return err
}

func handUpload(fname string, pos int) {
	var err error
	var file *os.File
	var iSize int
	file, err = os.Open(fname)
	if err != nil {
		log.Println("OpenFile failed ", fname, err)
		lFileMap.SetFileUploading(fname,false)
		return
	}

	iSize, err = syncf.GetFileSize(file)
	if err != nil {
		log.Println("syncf.GetFileSize failed ", fname, err)
		lFileMap.SetFileUploading(fname,false)
		return
	}

	defer func() {
		_ = file.Close()
		if pos != iSize && iSize != 0 {
			time.Sleep(time.Second*5)  // try again later
			fileChangeMap.AddFile(fname)
		}
	}()

	var connection *syncf.Connection
	connection, err = connPool.Get(clientCfg.RemoteAddr)
	if err != nil {
		log.Println("connPool.Get failed ", err)
		lFileMap.SetFileUploading(fname,false)
		return
	}

	connPutFlag := true
	defer func() {
		if connPutFlag {
			var errConn error
			connPool.Put(clientCfg.RemoteAddr, connection, &errConn)
		}
		lFileMap.SetFileUploading(fname,false)
	}()



	buf := cBufPool.Get().([]byte)
	defer func() {
		cBufPool.Put(buf)
	}()

	iReadSize := CommonFileReadSize
	iFileType := CheckFileType(fname)
	if iFileType != FileCommon {
		iReadSize= DataFileReadSize
	}

	for pos < iSize {
		_, err = file.Seek(int64(pos), 0)
		if err != nil {
			log.Println("file.Seek failed ", fname, err)
			return
		}

		var nr int
		nr, err = file.Read(buf[0:iReadSize])
		if err != nil {
			log.Println("file.Read failed ", fname, err)
			return
		}

		buf = buf[:nr]
		var data []byte
		data, err = PackData(buf, fname, pos, iFileType)
		if err != nil {
			log.Println("PackData failed ", fname, err)
			return
		}

		connection.ExtendDeadline()
		_, err = connection.Conn.Write(data)
		if err != nil {
			log.Println("Conn.Write failed ", err, connection.Conn.RemoteAddr(),
				fname, len(data))
			connPutFlag = false
			return
		}

		log.Println("Send data succeed, pos", pos, " size", len(buf), fname)

		var n int
		connection.ExtendDeadline()
		n, err = connection.Conn.Read(buf[0:iReadSize])
		if err != nil {
			log.Println("Conn.Read failed ", err, connection.Conn.RemoteAddr())
			connPutFlag = false
			return
		}

		buf = buf[0:n]
		var icount, iRst, iRspPos int
		icount, err = fmt.Sscanf(string(buf), "%d %d\n", &iRst, &iRspPos)
		if icount != 2 || err != nil {
			log.Println("fmt.Sscanf failed ", err, fname)
			return
		}

		if iRst != syncf.Succeed && iRst != syncf.FilePosErr {
			log.Println("Rsp err ", iRst, iRspPos)
			return
		}

		if iRst == syncf.FilePosErr {
			pos = iRspPos
		} else {
			pos += nr
		}

		iSize, err = syncf.GetFileSize(file)
		if err != nil {
			return
		}

		lFileMap.UpdateFileP(fname, pos, iSize, true)
	}


}

func PackData(buf []byte, fname string, pos int, ftype int) ([]byte, error) {
	var data []byte
	var bCmpress bool
	if ftype == FileCommon && len(buf) > 200 {
		data = syncf.GzipCompress(buf)
		if len(data) == 0 {
			return nil, errors.New("syncf.GzipCompress failed")
		}
		bCmpress = true
	} else {
		data = buf
	}

	strPath := clientCfg.GetSvrFullPath(fname)
	if len(strPath) == 0 {
		return nil, errors.New("server path get failed")
	}

	strHead := fmt.Sprintf("%s %d %d %d %t\n", strPath, pos, len(data), len(data), bCmpress)

	return append([]byte(strHead), data...), nil
}