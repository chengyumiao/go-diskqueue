package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
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
	options.MaxBytesPerFile = 256 * 1024 * 1024
	options.RollTimeSpanSecond = 3
	options.LimiterBatch = 1000
	options.LimiterDuration = 100 * time.Millisecond

	wal := diskqueue.NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		fmt.Println("start err", err)
	}

	ticker := time.NewTicker(7 * time.Second)
	exit := false

	writeCount := 0
	for !exit {
		select {
		case <-ticker.C:
			exit = true
		default:
			err := wal.Put([]byte(TestMessage))
			if err != nil {
				fmt.Println("Put error", err)
			}
			writeCount++

		}

	}

	wal.Close()

	time.Sleep(time.Second)

	fmt.Println("finish write......................................................................................")

	start := time.Now()
	wal = diskqueue.NewTimeRollQueue(l, options)

	repairCount := 0
	rpf := func(msg []byte) bool {
		repairCount++
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
		time.Sleep(time.Second)

		statInfo := wal.Stats()
		stat := reflect.TypeOf(*statInfo)
		value := reflect.ValueOf(*statInfo)

		for i := 0; i < stat.NumField(); i++ {
			field := stat.Field(i)
			key := statInfo.Name + field.Tag.Get("json")
			if key == "Name" {
				continue
			}

			value := value.Field(i).Interface()
			fmt.Println(key)
			fmt.Println(value)
		}

		statsBytes, _ := json.MarshalIndent(wal.Stats(), " ", " ")
		fmt.Println("stats", string(statsBytes))
	}

	wal.Close()

	speedSecond := time.Since(start).Seconds()

	fmt.Println("repair speed time: ", speedSecond, "write count: ", writeCount, "read count: ", repairCount)

}
