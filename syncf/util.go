package syncf

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type PathFileReq struct {
	RPaths []string `json:"rpaths"`
}

type PathFileRsq struct {
	Result int `json:"result"`
	Pathfiles []PathFiles `json:"pathfiles"`
}

type PathFiles struct {
	Path string `json:"path"`
	Files []FileStat `json:"files"`
}

type FileStat struct {
	FileName string `json:"filename"`
	Size int `json:"size"`
}

var gzipWriterPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}

func GzipCompress(msg []byte) []byte {
	var gzipBuf bytes.Buffer
	gzipWrite := gzipWriterPool.Get().(*gzip.Writer)
	gzipWrite.Reset(&gzipBuf)
	defer gzipWriterPool.Put(gzipWrite)

	_, err := gzipWrite.Write(msg)
	if err != nil {
		log.Println("gzipWrite error:" + err.Error())
		return nil
	}
	gzipWrite.Close()

	//log.Println("gzipWrite n:", n, " ori n:", len(msg), " gzip n:", gzipBuf.Len(), ",", len(gzipBuf.Bytes()))
	return gzipBuf.Bytes()
}

func GzipUnCompress(buf []byte) []byte {
	gzipRead, err := gzip.NewReader(bytes.NewReader(buf))
	if err != nil {
		log.Println("gzip.NewReader error:" + err.Error())
		return nil
	}

	decodeBuf, err := ioutil.ReadAll(gzipRead)
	if err != nil {
		log.Println("ReadAll(gzipRead) error:" + err.Error())
		return nil
	}

	return decodeBuf
}

func DecodeAndUnCompress(message []byte) []byte {
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(message)))
	n, err := base64.StdEncoding.Decode(buf, message)
	if err != nil {
		log.Println("base64 Decode error:" + err.Error())
		return nil
	}
	return GzipUnCompress(buf[:n])
}

func CompressAndEncode(message []byte) []byte {
	buf := GzipCompress(message)
	if len(buf) == 0 {
		return nil
	}

	enBuf := make([]byte, base64.StdEncoding.EncodedLen(len(buf)))
	base64.StdEncoding.Encode(enBuf, buf)
	return enBuf
}

func LoadConfig(cfgFileName string, cfg interface{}) bool {
	log.Println("LoadConfig cfgFileName: " + cfgFileName)

	if cfgFileName == "" {
		log.Println("LoadConfig config file name is empty")
		return false
	}
	cfgPath, err := filepath.Abs(cfgFileName)
	if err != nil {
		log.Println("filepath.Abs " + cfgFileName + " error:" + err.Error())
		return false
	}

	fd, err := os.Open(cfgPath)
	if err != nil {
		log.Println("Open " + cfgPath + " error:" + err.Error())
		return false
	}
	defer fd.Close()

	decoder := json.NewDecoder(fd)
	if err = decoder.Decode(cfg); err != nil {
		log.Println("Open " + cfgPath + " error:" + err.Error())
		return false
	}

	log.Println("LoadConfig cfgFileName: " + cfgFileName + " Success")

	return true
}
