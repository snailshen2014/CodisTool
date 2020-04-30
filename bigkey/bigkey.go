package bigkey

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/garyburd/redigo/redis"
	"koolearn.com/CodisTools/codisclient"
	"koolearn.com/CodisTools/utils"
)

//BigKey for replication keys
type BigKey struct {
	applicationName string
	zkAddr          string
	size            string
	productName     string
}

//NewBigKey for instanence Replication
func NewBigKey() *BigKey {
	return new(BigKey)
}

//Do for entry
func (r *BigKey) Do(d map[string]interface{}) {
	r.applicationName, _ = d["--applicationName"].(string)
	r.zkAddr, _ = d["--zkAddr"].(string)
	r.productName, _ = d["--productName"].(string)
	r.size, _ = d["--size"].(string)
	r.checkBigKey()

}

func getKeySize(key, keyType string) int {
	pool := codisclient.GetPool()
	client := pool.Get()
	defer client.Close()
	length := 0
	switch keyType {
	case "string":
		originalValue, error := redis.Int(client.Do("STRLEN", key))
		if error == nil {
			length = originalValue
			//log
			// fmt.Printf("###### getKeySize key %s size %d.\n", key, length)
		}
	case "list":
		originalValue, error := redis.Int(client.Do("LLEN", key))
		if error == nil {
			length = originalValue
			//log
			// fmt.Printf("###### getKeySize key %s size %d.\n", key, length)
		}
	case "set":
		originalValue, error := redis.Int(client.Do("SCARD", key))
		if error == nil {
			length = originalValue
			//log
			// fmt.Printf("###### getKeySize key %s size %d.\n", key, length)
		}
	case "zset":
		originalValue, error := redis.Int(client.Do("ZCARD", key))
		if error == nil {
			length = originalValue
			//log
			// fmt.Printf("###### getKeySize key %s size %d.\n", key, length)
		}
	case "hash":
		originalValue, error := redis.Int(client.Do("HLEN", key))
		if error == nil {
			length = originalValue
			//log
			// fmt.Printf("###### getKeySize key %s size %d.\n", key, length)
		}
	default:
		fmt.Printf("###### key:%s unknown key type:%s\n", key, keyType)
	}
	return length
}

func getKeyType(key string) string {
	return codisclient.KeyType(key)
}

func findBigKey(keys chan string, limitSize int, wg *sync.WaitGroup) {
	count := 0
	for {
		key, ok := <-keys
		if !ok {
			break
		}
		count++
		keyType := getKeyType(key)
		keySize := getKeySize(key, keyType)
		if keyType == "string" {
			if keySize > 10*1000 {
				fmt.Printf("###### Finded big key: %s ,rela size: %d,set size:%d,deladed number:%d\n", key, keySize, 10000, count)
				content := key + "," + strconv.Itoa(keySize) + ",type string\n"
				utils.Write("bigKey.txt", content)
			}
		} else {
			if keySize > limitSize {
				fmt.Printf("###### Finded big key: %s ,rela size: %d,set size:%d,deladed number:%d \n", key, keySize, limitSize, count)
				content := key + "," + strconv.Itoa(keySize) + ",type " + keyType + "\n"
				utils.Write("bigKey.txt", content)
			}
		}
	}
	// exitChan <- true
	// close(exitChan)
	wg.Done()
}

func (r *BigKey) checkBigKey() {
	limitSize, _ := strconv.Atoi(r.size)
	codisclient.SetZkInfo(r.productName, r.zkAddr)
	keysChan := make(chan string, 300000)
	var maxChanNum int = 50
	wg := sync.WaitGroup{}
	wg.Add(maxChanNum)

	// exitChan := make(chan bool, 1)
	go codisclient.SscanSlots(r.applicationName, keysChan)
	for chanNum := 0; chanNum < maxChanNum; chanNum++ {
		fmt.Printf("new goroutine num:%d\n", chanNum)
		go findBigKey(keysChan, limitSize, &wg)
	}

	// for {
	// 	_, ok := <-exitChan
	// 	if !ok {
	// 		break
	// 	}
	// }
	wg.Wait()
	fmt.Printf("######BigKey checking finished.\n")
}

//GetKeySize ,get key size
func GetKeySize(key string, client redis.Conn) int {
	keyType := codisclient.KeyType(key)
	var length = 0
	switch keyType {
	case "string":
		originalValue, error := redis.Int(client.Do("STRLEN", key))
		if error == nil {
			length = originalValue
			//log
			fmt.Printf("###### checkBigKey key %s size %d.\n", key, length)
		}
	case "list":
		originalValue, error := redis.Int(client.Do("LLEN", key))
		if error == nil {
			length = originalValue
			//log
			fmt.Printf("###### checkBigKey key %s size %d.\n", key, length)
		}
	case "set":
		originalValue, error := redis.Int(client.Do("SCARD", key))
		if error == nil {
			length = originalValue
			//log
			fmt.Printf("###### checkBigKey key %s size %d.\n", key, length)
		}
	case "zset":
		originalValue, error := redis.Int(client.Do("ZCARD", key))
		if error == nil {
			length = originalValue
			//log
			fmt.Printf("###### checkBigKey key %s size %d.\n", key, length)
		}
	case "hash":
		originalValue, error := redis.Int(client.Do("HLEN", key))
		if error == nil {
			length = originalValue
			//log
			fmt.Printf("###### checkBigKey key %s size %d.\n", key, length)
		}
	default:
		fmt.Printf("###### key:%s unknown key type:%s\n", key, keyType)
	}
	return length

}
