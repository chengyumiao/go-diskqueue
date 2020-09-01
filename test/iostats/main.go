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

	real_dir := path.Join(TEST_DIR_PREFIX, testDir)
	os.MkdirAll(real_dir, 0755)
	options.DataPath = real_dir
	options.Name = testDir

	wal := diskqueue.NewTimeRollQueue(l, options)

	err := wal.Start()
	if err != nil {
		panic("start err" + err.Error())
	}

	ticker := time.NewTicker(duration)

	slice_1 := []byte{'a'}
	writeBytes := bytes.Repeat(slice_1, 200*1000)

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

	real_dir := path.Join(TEST_DIR_PREFIX, testDir)
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
	testDirPrefix := "IOSTATS"

	wgWrite := &sync.WaitGroup{}

	for i := 0; i < 4; i++ {
		wgWrite.Add(1)
		go write(wgWrite, 5*time.Minute, testDirPrefix+strconv.Itoa(i))
	}
	// 睡眠一会儿等待
	time.Sleep(120 * time.Second)
	wgRead := &sync.WaitGroup{}

	fmt.Println(string(bytes.Repeat([]byte{'-'}, 100)))

	for i := 0; i < 4; i++ {
		wgRead.Add(1)
		go read(wgRead, time.Second*120, testDirPrefix+strconv.Itoa(i))
	}

	wgWrite.Wait()
	wgRead.Wait()

}
