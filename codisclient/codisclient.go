package codisclient

import (
	"fmt"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	codis "github.com/tranch-xiao/redigo-codis"
)

var pool *codis.Pool

var zkAddr string
var productName string

//SetZkInfo ,set productName and addr
func SetZkInfo(pName, addr string) {
	productName = pName
	zkAddr = addr
	initPool()
}

//GetPool ,for get codis pool
func GetPool() *codis.Pool {
	return pool
}

func initPool() {
	fmt.Printf("Zk info ,addr:%s,zkdir:%s\n", zkAddr, "/codis3/"+productName+"/proxy")
	zkdir := "/codis3/" + productName + "/proxy"
	pool = &codis.Pool{
		ZkServers: strings.Split(zkAddr, ","),
		ZkTimeout: time.Second * 60,
		ZkDir:     zkdir,
		Dial: func(network, address string) (redis.Conn, error) {
			conn, err := redis.Dial(network, address)
			if err != nil {
				conn.Send("AUTH", "PASSWORD")
			}
			return conn, err
		},
	}
}

//SscanSlots for slotsscan all slots for codis
func SscanSlots(from string, keys chan string) {
	client := pool.Get()
	defer client.Close()

	allSlotSum := 0
	for slot := 0; slot < 1024; slot++ {
		start := time.Now()
		cursor := 0
		slotNum := 0
		for {
			resultValue, err := redis.Values(client.Do("SLOTSSCAN", slot, cursor))
			if err != nil {
				fmt.Println(err)
				continue
			}
			slotKeys, ok := redis.Strings(resultValue[1], nil)
			if ok != nil {
				fmt.Printf("SLOTSSCAN slot:%d,cursor:%d,error:%s\n", slot, cursor, ok)
				continue
			}
			slotNum += len(slotKeys)
			for _, key := range slotKeys {
				if strings.HasPrefix(key, from) {
					keys <- key
				}

			}
			//
			//fmt.Printf("length is %d\n",len(result_value))
			// fmt.Printf("%T %T %T \n", result_value, result_value[0], result_value[1])
			// fmt.Println(result_value)
			// cursor, _ = strconv.Atoi(utils.B2S(result_value[0].([]uint8)))
			// for k, v := range result_value {
			// 	// fmt.Printf("k type:%T,k value:%v,v type:%T,v value:%v\n", k, k, v, v)
			// 	switch vv := v.(type) {
			// 	case string:
			// 		fmt.Println(k, "is string", vv)
			// 		keys.PushBack(v)
			// 	case float64:
			// 		fmt.Println(k, "is float", int64(vv))
			// 	case int:
			// 		fmt.Println(k, "is int", vv)
			// 	case []uint8:
			// 		// fmt.Printf("######slot:%d ,cursor is:%d\n", slot, cursor)
			// 	case []interface{}:
			// 		for _, u := range vv {
			// 			//fmt.Println(i, u)
			// 			key := utils.B2S(u.([]uint8))
			// 			slotNum++
			// 			if strings.HasPrefix(key, from) {
			// 				// fmt.Printf("add key is: %s\n", key)
			// 				keys.PushBack(key)
			// 			}
			// 		}
			// 	case nil:
			// 		fmt.Println(k, "is nil", "null")
			// 	case map[string]interface{}:
			// 		fmt.Println(k, "is an map:")
			// 	default:
			// 		fmt.Println(k, "is of a type I don't know how to handle ", fmt.Sprintf("%T", v))
			// 	}
			//fmt.Printf("#### %T %T %v %v\n",k,v,k,v)
			//fmt.Printf("length is %d\n",len(v))
			//for _,v2 := range v {
			//	fmt.Printf("##### %v\n",v2)
			// }
			cursor, _ = redis.Int(resultValue[0], nil)
			if cursor == 0 {
				cost := time.Since(start)
				fmt.Printf("######slot:%d sscan finished,keys size:%d,time cost:%v\n",
					slot, slotNum, cost)

				allSlotSum += slotNum
				break
			}

		}
		//if len(result_value) >= 2  {
		//	fmt.Printf("string byte  %s,%s\n",result_value[0],result_value[1])
		//}
		//fmt.Println(result_value)

		//}

	}
	fmt.Printf("######all slots sscan finished,keys size:[%d]\n", allSlotSum)
	close(keys) //close channel
}

//KeyType , return key type
func KeyType(key string) string {
	client := pool.Get()
	defer client.Close()
	result, err := redis.String(client.Do("type", key))
	if err == nil {
		return result
	}
	return ""
}
