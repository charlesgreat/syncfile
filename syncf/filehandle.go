package syncf

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	Succeed     int = iota
	FileNotExist
	FieleCreateErr
	FileRemoveErr
	FileWriteErr
	FileUsing
	FilePosErr
)

const (
	FileInfoCanUse int = iota
	FileInfoUsing
	FileInfoNo
)

var (
	FileOprErr = []byte{'0', '1', '2', '3', '4', '5', '6', '7'}
)

type FileHandleInfo struct {
	File *os.File
	time time.Time
	bUse bool
}


type FileHandleMap struct {
	sync.RWMutex
	Map map[string]*FileHandleInfo
}


func CheckFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

func CreateFilePath(path string) bool {
	if !CheckFileIsExist(path) {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return false
		}

		return true
	}
	return true
}

// file name with path, only create path
func CreateFilePathF(fileName string) bool {
	iLen := len(fileName)
	if iLen < 5 {
		return false
	}
	var path string
	if fileName[iLen-1] != '/' {
		iPos := strings.LastIndex(fileName, "/")
		if iPos < 0 {
			return false
		}
		path = fileName[0:iPos]
	} else {
		path = fileName
	}
	return CreateFilePath(path)
}



func (fileHandleMap *FileHandleMap) AddFileHandleInfo(fileName string, file *os.File) bool {
	fileHandleMap.Lock()
	defer fileHandleMap.Unlock()

	if _, isExist := fileHandleMap.Map[fileName]; !isExist {
		fileInfo := new(FileHandleInfo)
		fileInfo.File = file
		fileInfo.time = time.Now()
		fileInfo.bUse = false
		fileHandleMap.Map[fileName] = fileInfo
		return true
	} else {
		log.Println("AddFileInfo "+ " already exist " + fileName)
		return false
	}
}

func (fileHandleMap *FileHandleMap) RemoveFileHandleInfo(fileName string) {
	fileHandleMap.Lock()
	defer fileHandleMap.Unlock()

	if file, isExist := fileHandleMap.Map[fileName]; isExist {
		file.File.Close()
		delete(fileHandleMap.Map, fileName)
	}
}

func (fileHandleMap *FileHandleMap) CheckUnUsedFileHandle(sec int) {
	timer := time.NewTicker(time.Second * 10)
	var fileList []string
	var now time.Time
	var dur time.Duration
	for {
		select {
		case <-timer.C:
			fileHandleMap.Lock()
			now = time.Now()
			for fname, fInfo := range fileHandleMap.Map {
				if fInfo.bUse == true {
					continue
				}
				dur = now.Sub(fInfo.time)
				if int(dur.Seconds()) > sec {
					fileList = append(fileList, fname)
				}
			}
			for i := 0; i < len(fileList); i++{
				log.Println("FileHandleMap.CheckUnUsedFileHandle delte *File", sec, fileList[i])
				delete(fileHandleMap.Map, fileList[i])
			}
			fileList = fileList[0:0]
			fileHandleMap.Unlock()
		}
	}
}



func (fileHandleMap *FileHandleMap) GetFileHandleInfo(fileName string) (int, *FileHandleInfo) {
	fileHandleMap.Lock()
	defer fileHandleMap.Unlock()

	if fInfo, isExist := fileHandleMap.Map[fileName]; isExist {
		if fInfo.bUse {
			return FileInfoUsing, nil
		} else {
			fInfo.bUse = true
			return FileInfoCanUse, fInfo
		}
	}

	return FileInfoNo, nil

}

func (fileHandleMap *FileHandleMap) PutFileHandleInfo(fInfo *FileHandleInfo) {
	fileHandleMap.Lock()
	defer fileHandleMap.Unlock()
	fInfo.time = time.Now()
	fInfo.bUse = false
}

func CheckPosAndWrte(file *os.File, pos int, data []byte) (int, int) {
	//check pos
	stfileInfo, err := file.Stat()
	if err != nil {
		return FileNotExist, 0
	}

	iSize := (int)(stfileInfo.Size())
	if iSize != pos {
		if pos > iSize {
			return FilePosErr, iSize
		} else {
			err = os.Truncate(file.Name(), int64(pos))
			if err != nil {
				return FileWriteErr, 0
			}
		}

	}

	//write data
	iRst := 0
	iRst, err = file.WriteAt(data, int64(pos))
	if err != nil {
		return FileWriteErr, 0
	}

	return Succeed, iRst

}

func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func GetPathFileStat(path string) (fileStat []FileStat) {
	if !IsDir(path) {
		return nil
	}

	dirList, err := ioutil.ReadDir(path)
	if err != nil {
		return nil
	}
	for _, v := range dirList {
		if v.IsDir() {
			continue
		}
		fname := v.Name()
		if fname[0] == '.' || strings.Index(fname, " ") >=0 {
			continue
		}

		file := FileStat{v.Name(), int(v.Size())}
		fileStat = append(fileStat, file)
	}
	return fileStat
}

func GetFileStat(fname string) (fileStat FileStat, err error) {
	s, err := os.Stat(fname)
	if err != nil {
		return fileStat, err
	}

	if s.IsDir() {
		return fileStat, errors.New("it's dir")
	}

	fileStat.FileName = fname
	fileStat.Size = (int)(s.Size())
	return fileStat, err


}

func GetFileSize(file *os.File) (int, error) {
	fstat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fstat.Size()), nil
}