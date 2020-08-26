package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	"github.com/chengyumiao/go-diskqueue"
)

var TestMessage = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// 测试 repair功能
// 1. 构造一定量的数据写入
// 2. 关闭队列
// 3. 打开队列，启动repair程序
func main() {

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	l := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		fmt.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}

	options := diskqueue.DefaultOption()
	dirPrefix := fmt.Sprintf("Repair-%d", time.Now().UnixNano())
	tmpDir, err := ioutil.TempDir("", dirPrefix)
	if err != nil {
		panic(err)
	}

	defer func() {
		os.RemoveAll(tmpDir)
	}()

	options.DataPath = tmpDir
	options.Name = "Repair"
	options.MaxBytesPerFile = 5 * 1024 * 1024
	options.RollTimeSpanSecond = 3

	wal := diskqueue.NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		fmt.Println("start err", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	exit := false
	for !exit {
		select {
		case <-ticker.C:
			exit = true
		default:
			err := wal.Put([]byte(TestMessage))
			if err != nil {
				fmt.Println("Put error", err)
			}
		}

	}

	wal.Close()

	time.Sleep(time.Second)

	fmt.Println("finish write......................................................................................")

	wal = diskqueue.NewTimeRollQueue(l, options)

	rpf := func(msg []byte) bool {
		// fmt.Println(string(msg))
		return true
	}

	wal.SetRepairProcessFunc(rpf)

	err = wal.Start()
	if err != nil {
		fmt.Println("start err", err)
	}

	for {
		if wal.FinishRepaired() {
			fmt.Println("finish...")
			break
		}
	}

	wal.Close()

	time.Sleep(10000 * time.Second)

}
