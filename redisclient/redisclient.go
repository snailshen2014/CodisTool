package redisclient

import (
	"fmt"
	"github.com/go-redis/redis"
	"strings"
)

var client *redis.Client
var serverAddr string

var clients map[string]*redis.Client = make(map[string]*redis.Client, 10)

//SetServerAddr ,
func SetServerAddr(addr string) {
	serverAddr = addr
}

//GetRedisClient ,
func GetRedisClient() *redis.Client {
	initClient()
	return client
}

func initClient() {
	client = redis.NewClient(&redis.Options{
		Addr:     serverAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
}

//InitServerClients ,
func InitServerClients(servers []string) {
	for _, server := range servers {
		SetServerAddr(server)
		client := GetRedisClient()
		clients[server] = client
	}
}

//CloseClients ,
func CloseClients() {
	for server, client := range clients {

		if ok := client.Close(); ok == nil {
			fmt.Printf("Close server:[%s] ok.", server)
		}
	}
}

//GetKeySizeByKey for getting key size ,https://www.cnblogs.com/ExMan/p/11586751.html,./redis-cli -h 10.155.10.156 -p 11000 --bigkeys
func GetKeySizeByKey(key string) string {
	var size string
	for _, client := range clients {
		result := client.DebugObject(key)
		resultStr := result.Val()
		if strings.Contains(resultStr, "serializedlength") {
			begin := strings.Index(resultStr, "serializedlength")
			end := strings.Index(resultStr, "lru")
			size = strings.TrimSpace(resultStr[begin+17 : end])
			// fmt.Printf("###GetKeySizeByKey size:[%s],resulstStr:[%s]\n", size, resultStr)
			break
		}

	}

	return size
}
