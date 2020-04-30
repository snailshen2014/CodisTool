package replication

import (
	"fmt"
	"strings"

	"github.com/go-redis/redis"
	"koolearn.com/CodisTools/codisclient"
	"koolearn.com/CodisTools/redisclient"
	"koolearn.com/CodisTools/utils"
)

//Replication for replication keys
type Replication struct {
	fromKey     string
	toKey       string
	serverAddr  string
	zkAddr      string
	productName string
}

//NewReplication for instanence Replication
func NewReplication() *Replication {
	return new(Replication)
}

//Do for entry
func (r *Replication) Do(d map[string]interface{}) {
	r.serverAddr, _ = d["--serverAddr"].(string)
	r.fromKey, _ = d["--fromKey"].(string)
	r.toKey, _ = d["--toKey"].(string)
	r.zkAddr, _ = d["--zkAddr"].(string)
	r.productName, _ = d["--productName"].(string)
	r.replicationKey(d)
}

func replication(keys chan string, exitChan chan bool, to string) {
	for {
		key, ok := <-keys
		if !ok {
			break
		}
		orginalKey := key
		keyType := codisclient.KeyType(orginalKey)
		businessKey := orginalKey[strings.Index(orginalKey, "_")+1:]
		var targetKey = to + ":" + businessKey
		pool := codisclient.GetPool()
		client := pool.Get()
		defer client.Close()
		switch keyType {
		case "string":
			//replication
			originalValue, error := client.Do("GET", orginalKey)
			if error == nil {
				_, setTargetError := client.Do("SET", targetKey, utils.B2S(originalValue.([]uint8)))
				if setTargetError != nil {
					//log
					fmt.Printf("###### key from %s to %s  replication error.\n", orginalKey, targetKey)
				}
				//log
				fmt.Printf("###### key from %s to %s finished replication.\n", orginalKey, targetKey)
			}
		case "list":
			fmt.Println("Key type:", "list")
			originalValue, error := client.Do("lrange", orginalKey, 0, -1)
			if error == nil {
				fmt.Printf("originalValue type:%T\n", originalValue)
				fmt.Printf("originalValue type:%s\n", originalValue)
				for k, v := range utils.Transfer(originalValue) {
					fmt.Printf("%v %v\n", k, v)
					_, setTargetError := client.Do("rpush", targetKey, v)
					if setTargetError != nil {
						//log
						fmt.Printf("###### key from %s to %s  replication error.\n", orginalKey, targetKey)
					}
				}

				//log
				fmt.Printf("###### key from %s to %s finished replication.\n", orginalKey, targetKey)
			}
		case "set":
			fmt.Println("Key type:", "set")
			originalValue, error := client.Do("smembers", orginalKey)
			if error == nil {
				fmt.Printf("originalValue type:%T\n", originalValue)
				fmt.Printf("originalValue type:%s\n", originalValue)
				for k, v := range utils.Transfer(originalValue) {
					fmt.Printf("%v %v\n", k, v)
					_, setTargetError := client.Do("sadd", targetKey, v)
					if setTargetError != nil {
						//log
						fmt.Printf("###### key from %s to %s  replication error.\n", orginalKey, targetKey)
					}
				}

				//log
				fmt.Printf("###### key from %s to %s finished replication.\n", orginalKey, targetKey)
			}
		case "zset":
			fmt.Println("Key type:", "zset")
			originalValue, error := client.Do("zrange", orginalKey, 0, -1)
			if error == nil {
				fmt.Printf("originalValue type:%T\n", originalValue)
				fmt.Printf("originalValue type:%s\n", originalValue)
				for k, v := range utils.Transfer(originalValue) {
					fmt.Printf("%v %v\n", k, v)
					_, setTargetError := client.Do("zadd", targetKey, v)
					if setTargetError != nil {
						//log
						fmt.Printf("###### key from %s to %s  replication error.\n", orginalKey, targetKey)
					}
				}

				//log
				fmt.Printf("###### key from %s to %s finished replication.\n", orginalKey, targetKey)
			}
		case "hash":
			fmt.Println("Key type:", "hash")
			originalValue, error := client.Do("hkeys", orginalKey)
			if error == nil {
				fmt.Printf("originalValue type:%T\n", originalValue)
				fmt.Printf("originalValue type:%s\n", originalValue)
				for _, hashkey := range utils.Transfer(originalValue) {
					field := utils.B2S(hashkey.([]uint8))
					fmt.Printf("key field:%s\n", field)
					hashvalue, hgetError := client.Do("hget", orginalKey, field)
					fmt.Printf("hashvalue:%v \n", hashvalue)
					fmt.Println(hgetError)
					if hgetError == nil {
						_, hsetError := client.Do("hset", targetKey, field, hashvalue)
						if hsetError != nil {
							//log
							fmt.Printf("###### key from %s to %s  replication error.\n", orginalKey, targetKey)
						}
					}
				}

				//log
				fmt.Printf("###### key from %s to %s finished replication.\n", orginalKey, targetKey)
			}
		default:
			fmt.Println("unknown key type.")
		}
	}
	exitChan <- true
	close(exitChan)
}

func (r *Replication) replicationKey(d map[string]interface{}) {
	switch {
	case d["--serverAddr"] != nil:
		fmt.Println("sesersfsfs")
		redisclient.SetServerAddr(r.serverAddr)
		client := redisclient.GetRedisClient()
		err := client.Set("key", "value", 0).Err()
		if err != nil {
			panic(err)
		}

		val, err := client.Get("key").Result()
		if err != nil {
			panic(err)
		}
		fmt.Println("key", val)

		val2, err := client.Get("key2").Result()
		if err == redis.Nil {
			fmt.Println("key2 does not exist")
		} else if err != nil {
			panic(err)
		} else {
			fmt.Println("key2", val2)
		}

		keys := client.Keys(r.fromKey + "*")
		fmt.Printf("keys size:[%d]\n", len(keys.Val()))
	case d["--zkAddr"] != nil:
		codisclient.SetZkInfo(r.productName, r.zkAddr)
		keysChan := make(chan string, 100000)
		exitChan := make(chan bool, 1)

		go codisclient.SscanSlots(r.fromKey, keysChan)
		go replication(keysChan, exitChan, r.toKey)

		// fmt.Printf("###### keys size :[%d]\n", keys.Len())
		for {
			_, ok := <-exitChan
			if !ok {
				break
			}
		}
	}

}
