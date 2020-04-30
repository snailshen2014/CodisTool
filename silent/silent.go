package silent

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"koolearn.com/CodisTools/codisclient"
	"koolearn.com/CodisTools/utils"
)

//SilentKey for scan silent keys
type SilentKey struct {
	applicationName string
	zkAddr          string
	time            string
	productName     string
	servers         string
}

//NewSilentKey for instanence SilentKey
func NewSilentKey() *SilentKey {
	return new(SilentKey)
}

//Do for entry
func (r *SilentKey) Do(d map[string]interface{}, servers string) {

	r.applicationName, _ = d["--applicationName"].(string)
	r.zkAddr, _ = d["--zkAddr"].(string)
	r.productName, _ = d["--productName"].(string)
	r.time, _ = d["--time"].(string)
	r.servers = servers
	r.checkSilentKey()

}

func (r *SilentKey) checkSilentKey() {
	start := time.Now()
	var maxChanNum int = 50
	expiredTime, _ := strconv.Atoi(r.time)
	codisclient.SetZkInfo(r.productName, r.zkAddr)

	keysChan := make(chan string, 200000)
	sumsize := make(chan float64, maxChanNum)

	wg := sync.WaitGroup{}
	wg.Add(maxChanNum)
	go codisclient.SscanSlots(r.applicationName, keysChan)
	for chanNum := 0; chanNum < maxChanNum; chanNum++ {
		fmt.Printf("new goroutine num:%d\n", chanNum)
		go findSilentKey(keysChan, expiredTime, r.servers, sumsize, &wg)
	}
	wg.Wait()
	close(sumsize)
	memorySize := 0.0

	for {
		size, ok := <-sumsize
		if !ok {
			break
		}
		fmt.Printf("get gorouting size:%f\n", size)
		memorySize += size
	}
	sF := fmt.Sprintf("%f", memorySize)
	content := "sum size" + "," + sF + "\n"
	utils.Write("silentKeys.txt", content)
	cost := time.Since(start)
	fmt.Printf("all slots keys checked  finishend,cost time:%v\n", cost)

}

func findSilentKey(keys chan string, expiredTime int, servers string, sumSize chan float64, wg *sync.WaitGroup) {
	// silents := make(map[string]int, 10000)
	var gSize float64 = 0.0

	for {
		key, ok := <-keys
		if !ok {
			break
		}
		ttl := getKeyTtl(key)
		if ttl == -1 || ttl >= expiredTime {
			//log
			// fmt.Printf("###### Silent key:%s ttl:%d bigger to set:%d.\n", key, ttl, expiredTime)
			// silents[key] = ttl
			// count++

			// remove ,for cpu alert
			// size := utils.GetKeyMemorySize(key, servers)
			// if s, err := strconv.ParseFloat(size, 64); err == nil {
			// 	gSize += s
			// }
			size := "0"
			content := key + "," + strconv.Itoa(ttl) + "," + size + "\n"
			utils.Write("silentKeys.txt", content)
		}

	}
	// if len(silents) > 0 {
	// 	//sort
	// 	sortkeys := utils.SortMapByValue(silents)
	// 	fmt.Printf("sortkeys size#######:%d\n", len(sortkeys))

	// 	// servers := []string{"10.155.10.156:11000"}
	// 	// redisclient.InitServerClients(servers)

	// 	for _, v := range sortkeys {
	// 		size := "0"
	// 		// fmt.Printf("key = %s,value = %d\n", v.Key, v.Value)
	// 		// size := redisclient.GetKeySizeByKey(v.Key)
	// 		//for call python tool
	// 		// fmt.Printf("###before key:%s\n", v.Key)
	// 		size = utils.GetKeyMemorySize(v.Key, r.servers)
	// 		if s, err := strconv.ParseFloat(size, 64); err == nil {
	// 			sumBytes += s
	// 		}

	// 		content := v.Key + "," + strconv.Itoa(v.Value) + "," + size + "\n"
	// 		utils.Write("silentKeys.txt", content)
	// 	}
	// }
	// sF := fmt.Sprintf("%f", sumBytes)
	// content := "sum size" + "," + sF + "\n"
	// utils.Write("silentKeys.txt", content)
	sumSize <- gSize
	wg.Done()
}

func getKeyTtl(key string) int {
	pool := codisclient.GetPool()
	client := pool.Get()
	defer client.Close()
	ttl, error := redis.Int(client.Do("ttl", key))
	// fmt.Printf("###### checkSilentKey key %s ttl %d .\n", orginalKey, ttl)
	if error == nil {
		return ttl
	}
	return -2
}
