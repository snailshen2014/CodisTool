/*
 * @Author: your name
 * @Date: 2020-02-24 17:55:04
 * @LastEditTime: 2020-03-05 17:38:31
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /CodisTools/prefix/prefixkey.go
 */
package prefix

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/garyburd/redigo/redis"
	"koolearn.com/CodisTools/codisclient"
	"koolearn.com/CodisTools/utils"
)

//PrefixKey for scan key's prefix ,prefix_xxx
type PrefixKey struct {
	zkAddr      string
	productName string
	prefix      string
	dump        bool
	del         bool
}

//NewPrefixKey for instanence prefixKey
func NewPrefixKey() *PrefixKey {
	return new(PrefixKey)
}

//Do for entry
func (r *PrefixKey) Do(d map[string]interface{}) {
	r.zkAddr, _ = d["--zkAddr"].(string)
	r.productName, _ = d["--productName"].(string)
	r.prefix, _ = d["--prefix"].(string)
	r.dump = d["--dump"].(bool)
	r.del = d["--del"].(bool)
	fmt.Printf("zkAddr:[%s],productName:[%s],prefix:[%s],dump:[%v],del:[%v]\n", r.zkAddr, r.productName,
		r.prefix, r.dump, r.del)
	r.findKeyPrefix()

}
func (r *PrefixKey) findKeyPrefix() {
	codisclient.SetZkInfo(r.productName, r.zkAddr)
	keysChan := make(chan string, 300000)

	var maxChanNum int = 20
	wg := sync.WaitGroup{}
	wg.Add(maxChanNum)

	// exitChan := make(chan bool, 1)
	go codisclient.SscanSlots(r.prefix, keysChan)
	for chanNum := 0; chanNum < maxChanNum; chanNum++ {
		fmt.Printf("new goroutine num:%d\n", chanNum)
		// go keyPrefix(keysChan, &wg, prefixChan)
		if r.del {
			go delKey(keysChan, &wg, "./del.log", r.productName, r.zkAddr)
		}
		if r.dump {
			go dumpKey(keysChan, &wg, "./dump.log", r.productName, r.zkAddr)
		}
	}
	wg.Wait()
	fmt.Printf("######key prefix checking finished.\n")
}

func keyPrefix(keys chan string, wg *sync.WaitGroup, prefixChan chan string) {
	keyPrefix := make(map[string]bool)
	for {
		key, ok := <-keys
		if !ok {
			break
		}
		elements := strings.Split(key, "_")
		prefix := elements[0]
		if len(prefix) == 0 {
			continue
		}
		_, exists := keyPrefix[prefix]
		if !exists {
			keyPrefix[prefix] = true
			prefixChan <- prefix
		}
		// fmt.Printf("elements:[%s],prefix:[%s],exists:[%v]\n", elements, prefix, exists)
	}

	wg.Done()
}

func delKey(keys chan string, wg *sync.WaitGroup, fileName, productName, zkAddr string) {
	codisclient.SetZkInfo(productName, zkAddr)
	pool := codisclient.GetPool()
	client := pool.Get()
	for {
		key, ok := <-keys
		if !ok {
			break
		}

		fmt.Printf("get key:[%s]\n", key)
		//del
		result, error := redis.Int(client.Do("del", key))
		if error != nil {
			//log
			fmt.Printf("###### del  key:[%s] error:[%v].\n", key, error)
		} else {

			if result == 1 {
				log.Printf("Key:[%s] is deleted ,return code:[%v].\n", key, result)
				utils.Tracefile(fileName, key+","+"ok")
			}
			if result == 0 {
				log.Printf("key:[%s] no deleted,key no exists ,return code:[%v].\n", key, result)
				utils.Tracefile(fileName, key+","+"no exists")
			}

		}

	}

	wg.Done()
}

func dumpKey(keys chan string, wg *sync.WaitGroup, fileName, productName, zkAddr string) {
	codisclient.SetZkInfo(productName, zkAddr)
	pool := codisclient.GetPool()
	client := pool.Get()
	for {
		key, ok := <-keys
		if !ok {
			break
		}
		keyType := codisclient.KeyType(key)
		//current,only support string type key
		if keyType == "string" {
			originalValue, error := client.Do("GET", key)
			if error == nil {
				sValue := utils.B2S(originalValue.([]uint8))
				row := key + "," + sValue
				fmt.Println("row:" + row)
				utils.Tracefile(fileName, row)
			} else {
				log.Printf("key:[%s] get error [%v].\n", key, error)
			}
		}

	}

	wg.Done()
}
