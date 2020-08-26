package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/chengyumiao/go-diskqueue"
)

func test1() {

	l := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		fmt.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
	options := diskqueue.DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("Concurrency-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "Concurrency"

	wal := diskqueue.NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		panic("start err" + err.Error())
	}

	wg := sync.WaitGroup{}

	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {

			for i := 0; i < 10000; i++ {
				err := wal.Put([]byte("a"))
				if err != nil {
					panic("Put error" + err.Error())
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()

	wal.Close()

	wal2I := diskqueue.NewTimeRollQueue(l, options)
	wal2, _ := wal2I.(*diskqueue.WALTimeRollQueue)

	wal2.Start()
	count := 0

	for {
		_, ok := wal2.ReadMsg()
		if ok {
			count++
		} else {
			break
		}
	}

	if count != 100000 {
		panic("read count not equal write count, read count" + strconv.Itoa(count))
	}
}

func test2() {

	l := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		fmt.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
	options := diskqueue.DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestConCurrencyWriteAndRead-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestConCurrencyWriteAndRead"

	wal := diskqueue.NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		log.Fatal("start err", err)
	}

	wg := sync.WaitGroup{}

	for j := 0; j < 1; j++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				err := wal.Put([]byte("a"))
				if err != nil {
					log.Fatal("Put error", err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	wal.Close()

	wal2I := diskqueue.NewTimeRollQueue(l, options)
	wal2I.Start()
	count := int32(0)

	wal2, _ := wal2I.(*diskqueue.WALTimeRollQueue)

	for j := 0; j < 5; j++ {
		wg.Add(1)
		go func() {
			for {
				msgBytes, ok := wal2.ReadMsg()
				if ok {
					atomic.AddInt32(&count, 1)
					if string(msgBytes) != "a" {
						log.Fatal("not equal a")
					}
				} else {
					break
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	if count != 1000 {
		log.Fatal("read count not equal write count", "read count", count)
	}
	wal2.Close()

}

func main() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	// test1()
	test2()
}
