package utils

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

//GetBytesByKey ,
//redis-memory-for-key -s 10.155.10.156 -p 11000 _sharks:product-ea:10446
//pip install rdbtools
func getBytesByKey(key, server string, port string) string {
	// args := []string{"redis-memory-for-key", "-s", server, "-p", port, key}
	// out, err := exec.Command("redis-memory-for-key", "-s", server, "-p", port, key).Output()
	// if err != nil {
	// 	fmt.Printf("###exec error,key:%s,error info:%s,out:%s\n", key, err, string(out))
	// 	return "0"
	// }
	cmd := exec.Command("redis-memory-for-key", "-s", server, "-p", port, key)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		//[ERROR] exit status 255Key donut.english_read_message:76487935 does not exist
		// log.Error(err.Error(), stderr.String())
		if strings.Contains(err.Error(), "255") {
			return "NO-EXIST"
		}
	}
	result := out.String()
	// fmt.Println("#### result:" + result)
	var bytesStr string
	if strings.Contains(result, "Bytes") {
		begin := strings.Index(result, "Bytes")
		end := strings.LastIndex(result, "Type")
		if begin == -1 || end == -1 {
			fmt.Printf("###key:%s,server:%s,port:%s,result:%s,error.\n", key, server, port, result)
		}
		bytesStr = result[begin+5 : end]
		// fmt.Printf("Bytes:%s \n", result[begin+5:end])
	}
	return strings.TrimSpace(bytesStr)
}

//GetKeyMemorySize , for get key size on the servers
func GetKeyMemorySize(key, servers string) string {
	var size string
	for _, server := range strings.Split(servers, ",") {
		if strings.Index(server, ":") == -1 {
			fmt.Printf("######server:%s,config error.\n", server)
			continue
		}
		ip := server[0:strings.Index(server, ":")]
		port := server[strings.Index(server, ":")+1:]
		size = getBytesByKey(key, ip, port)
		if size == "NO-EXIST" {
			fmt.Printf("### key:%s no exist on server:%s\n", key, server)
			continue
		} else {
			break
		}
	}
	return size
}
