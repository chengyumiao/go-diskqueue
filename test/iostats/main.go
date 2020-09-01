package main

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/chengyumiao/go-diskqueue"
)

const TEST_DIR_PREFIX = "/data/test"

func write(wg *sync.WaitGroup, duration time.Duration, testDir string) {

	defer wg.Done()

	l := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		fmt.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
	options := diskqueue.DefaultOption()

	real_dir := path.Join(TEST_DIR_PREFIX, fmt.Sprintf(testDir+"-%d", time.Now().UnixNano()))
	os.MkdirAll(real_dir, 0755)
	options.DataPath = real_dir
	options.Name = testDir

	wal := diskqueue.NewTimeRollQueue(l, options)

	err := wal.Start()
	if err != nil {
		panic("start err" + err.Error())
	}

	ticker := time.NewTicker(duration)

	slice_1 := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g'}
	writeBytes := bytes.Repeat(slice_1, 20)

OUT:
	for {
		select {
		case <-ticker.C:
			break OUT
		default:

			err := wal.Put(writeBytes)
			if err != nil {
				panic("Put error" + err.Error())
			}
		}
	}
}

func read(wg *sync.WaitGroup, duration time.Duration, testDir string) {

	defer wg.Done()

	l := func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
		fmt.Println(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
	options := diskqueue.DefaultOption()

	real_dir := path.Join(TEST_DIR_PREFIX, fmt.Sprintf(testDir+"-%d", time.Now().UnixNano()))
	options.DataPath = real_dir
	options.Name = testDir

	wal := diskqueue.NewTimeRollQueue(l, options)

	err := wal.Start()
	if err != nil {
		panic("start err" + err.Error())
	}

	ticker := time.NewTicker(duration)

OUT:
	for {
		select {
		case <-ticker.C:
			break OUT

		default:

			_, ok := wal.ReadMsg()
			if ok {
			} else {
				break OUT
			}
		}
	}

}

func main() {
	testDirPrefix := "IOSTATS_"

	wg := &sync.WaitGroup{}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go write(wg, time.Second*300, testDirPrefix+strconv.Itoa(i))
	}

	wg.Wait()

}
