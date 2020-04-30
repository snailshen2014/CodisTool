package delkey

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"koolearn.com/CodisTools/codisclient"
	"koolearn.com/CodisTools/utils"
	"log"
	"os"
	"time"
)

//DelKey for del key
type DelKey struct {
	zkAddr      string
	file        string
	productName string
}

//NewDelKey for instanence DelKey
func NewDelKey() *DelKey {
	return new(DelKey)
}

//Do for entry
func (r *DelKey) Do(d map[string]interface{}) {
	r.zkAddr, _ = d["--zkAddr"].(string)
	r.productName, _ = d["--productName"].(string)
	r.file, _ = d["--file"].(string)
	r.delKey()
}

func (r *DelKey) delKey() {
	fmt.Println("delKey....,")

	codisclient.SetZkInfo(r.productName, r.zkAddr)
	pool := codisclient.GetPool()
	client := pool.Get()

	records := utils.GetContent(r.file)
	var num int = 0
	for _, record := range records {
		// record = strings.Replace(record, "\n", "", -1)
		result, error := redis.Int(client.Do("del", record))
		if error != nil {
			//log
			fmt.Printf("###### del  key:[%s] error:[%v].\n", record, error)
		} else {
			if result == 1 {
				fmt.Printf("######Key:[%s] is deleted ,return code:[%v].\n", record, result)
				log.Printf("Key:[%s] is deleted ,return code:[%v].\n", record, result)
			}
			if result == 0 {
				fmt.Printf("######key:[%s] no deleted,key no exists ,return code:[%v].\n", record, result)
				log.Printf("key:[%s] no deleted,key no exists ,return code:[%v].\n", record, result)
			}

		}
		num++
		if num == 100 {
			time.Sleep(time.Second)
			num = 0
		}
	}

}

func init() {
	file := "./" + "log" + ".txt"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[DelKey]")
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.LUTC)
	return
}
