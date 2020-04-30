/*
 * @Author: your name
 * @Date: 2020-01-01 21:41:30
 * @LastEditTime: 2020-03-05 16:12:31
 * @LastEditors: Please set LastEditors
 * @Description: In User Settings Edit
 * @FilePath: /CodisTools/main.go
 */
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/docopt/docopt-go"
	"github.com/gopkg.in/ini.v1"
	"koolearn.com/CodisTools/bigkey"
	delkey "koolearn.com/CodisTools/delKey"
	"koolearn.com/CodisTools/prefix"
	"koolearn.com/CodisTools/replication"
	"koolearn.com/CodisTools/silent"
)

// 1.by redis server ./replication-tool  --serverAddr=10.155.10.156:11000 --fromKey=prifx --toKey=sdf
//2.by codis server ./replication-tool  --zkAddr=10.155.10.156:2181 --fromKey=prifx --toKey=sdf
func main() {
	const usage = `
Usage:
	codis-tool --replication --productName=pName (--serverAddr=ADDR |--zkAddr=ADDR ) --fromKey=from --toKey=to
	codis-tool --bigKey --productName=pName (--serverAddr=ADDR |--zkAddr=ADDR ) [--applicationName=NAME]--size=size
	codis-tool --delKey --productName=pName (--serverAddr=ADDR |--zkAddr=ADDR ) --file=Name
	codis-tool --silentKey --productName=pName (--serverAddr=ADDR |--zkAddr=ADDR ) --time=Seconds
	codis-tool --prefixKey --productName=pName (--serverAddr=ADDR |--zkAddr=ADDR ) --prefix=prefix (--dump|--del)

Options:
  -h --help     Show this screen.
  --version     Show version.

`
	fmt.Println(os.Args)
	d, err := docopt.ParseArgs(usage, os.Args[1:], "1.0")
	if err != nil {
		log.Fatal(err, "parse arguments failed")
		fmt.Println("parse error.")
	}
	switch {
	case d["--replication"].(bool):
		fmt.Println("replication")
		replication.NewReplication().Do(d)
	case d["--bigKey"].(bool):
		bigkey.NewBigKey().Do(d)
	case d["--delKey"].(bool):
		delkey.NewDelKey().Do(d)
	case d["--silentKey"].(bool):
		cfg, err := ini.Load("servers.ini")
		if err != nil {
			fmt.Printf("Fail to read file: %v", err)
			os.Exit(1)
		}
		servers := cfg.Section("server").Key("address").String()
		silent.NewSilentKey().Do(d, servers)
	case d["--prefixKey"].(bool):
		prefix.NewPrefixKey().Do(d)
	default:
		fmt.Println("Commond error.")
	}

	// c := make(chan os.Signal, 1)
	// // We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// // SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	// signal.Notify(c, os.Interrupt)

	// // Block until we receive our signal.
	// <-c
	// fmt.Println("codis-tool shutdown.")
}
