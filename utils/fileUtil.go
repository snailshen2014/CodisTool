/*
 * @Author: your name
 * @Date: 2020-01-06 16:18:52
 * @LastEditTime: 2020-03-05 17:31:47
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /CodisTools/utils/fileUtil.go
 */
package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

//Write,append content to fileName
func Write(fileName, content string) error {
	// 以只写的模式，打开文件
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("file:", "fileName , create failed. err: "+err.Error())
	} else {
		// 查找文件末尾的偏移量
		n, _ := f.Seek(0, os.SEEK_END)
		// 从末尾的偏移量开始写入内容
		_, err = f.WriteAt([]byte(content), n)
	}
	defer f.Close()
	return err
}

//GetContent read file to []string
func GetContent(filename string) []string {
	fileIn, fileInErr := os.Open(filename)
	if fileInErr != nil {
		fmt.Println("error!")
	}
	defer fileIn.Close()
	finReader := bufio.NewReader(fileIn)
	var fileList []string
	for {
		line, err := finReader.ReadString('\n')
		line = strings.TrimSpace(line)
		fmt.Println(line)
		if err == io.EOF {
			break
		}
		if err == nil && len(line) != 0 {
			fileList = append(fileList, line)
		}

	}
	//fmt.Println("fileList",fileList)
	return fileList
}

//Tracefile ,append file
func Tracefile(fileName, content string) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("file:", "fileName , create failed. err: "+err.Error())
	} else {
		timeStr := time.Now().Format("2006-01-02 15:04:05")
		row := timeStr + "," + content + "\n"
		buf := []byte(row)
		f.Write(buf)
	}
	defer f.Close()
	return err
}
