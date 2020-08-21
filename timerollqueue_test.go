package diskqueue

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 基本的写入读取的测试
func TestBasicWrite(t *testing.T) {
	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestBasicWrite-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestBasicWrite"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	for i := 0; i < 10; i++ {
		err := wal.Put([]byte("a"))
		if err != nil {
			t.Fatal("Put error", err)
		}
	}
	wal.Close()
}

func TestRepairQueue(t *testing.T) {

	l := NewTestLogger(t)
	options := DefaultOption()
	dirPrefix := fmt.Sprintf("TestRepairQueue-%d", time.Now().UnixNano())
	tmpDir, err := ioutil.TempDir("", dirPrefix)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.RemoveAll(tmpDir)
	}()

	options.DataPath = tmpDir
	options.Name = "TestRepairQueue"
	options.MaxBytesPerFile = 32 * 1024
	options.RollTimeSpanSecond = 5

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	ticker := time.NewTicker(16 * time.Second)
	exit := false
	for !exit {
		select {
		case <-ticker.C:
			exit = true
		default:
			err := wal.Put([]byte("a"))
			if err != nil {
				t.Fatal("Put error", err)
			}
		}

	}

	wal.Close()

	walRecover := &WALTimeRollQueue{
		Name:            options.Name,
		dataPath:        options.DataPath,
		maxBytesPerFile: options.MaxBytesPerFile,
		minMsgSize:      options.MinMsgSize,
		maxMsgSize:      options.MaxMsgSize,
		syncEvery:       options.SyncEvery,
		syncTimeout:     options.SyncTimeout,
		rollTimeSpan:    time.Duration(options.RollTimeSpanSecond) * time.Second,
		backoffDuration: options.BackoffDuration,
		logf:            l,
	}

	repairNames, err := walRecover.getAllRepairQueueNames()
	if err != nil {
		t.Fatal("getAllRepairQueueNames err ", err)
	}
	if len(repairNames) <= 0 {
		t.Fatal("repair names len err")
	}

	walRecover.init()

	if len(repairNames) >= 2 {
		if repairNames[1] != walRecover.getNextRepairQueueName(repairNames[0]) {
			t.Fatal("getNextRepairQueueName is err")
		}
	}

	for _, repairQueue := range walRecover.repairQueueNames {

		rq := New(repairQueue, walRecover.dataPath, walRecover.maxBytesPerFile, walRecover.minMsgSize, walRecover.maxMsgSize, walRecover.syncEvery, walRecover.syncTimeout, walRecover.logf)
		rqDiskQueue, ok := rq.(*diskQueue)
		if !ok {
			t.Fatal("change to diskqueue panic")
		}
		rqDiskQueue.readFileNum = rqDiskQueue.writeFileNum
		rqDiskQueue.readPos = rqDiskQueue.writePos
		err := rqDiskQueue.persistMetaData()
		if err != nil {
			t.Fatal("persistMetaData", err)
		}
	}

	walRecover.ResetRepairs()

	for _, repairQueue := range walRecover.repairQueueNames {

		rq := New(repairQueue, walRecover.dataPath, walRecover.maxBytesPerFile, walRecover.minMsgSize, walRecover.maxMsgSize, walRecover.syncEvery, walRecover.syncTimeout, walRecover.logf)
		rqDiskQueue, ok := rq.(*diskQueue)
		if !ok {
			t.Fatal("change to diskqueue panic")
		}

		if rqDiskQueue.readFileNum != 0 || rqDiskQueue.readPos != 0 {
			t.Fatal("readFileNum != 0 or readPos != 0")
		}

	}

	walRecover.ResetRepairs()

	for _, repairQueue := range walRecover.repairQueueNames {

		rq := New(repairQueue, walRecover.dataPath, walRecover.maxBytesPerFile, walRecover.minMsgSize, walRecover.maxMsgSize, walRecover.syncEvery, walRecover.syncTimeout, walRecover.logf)
		rqDiskQueue, ok := rq.(*diskQueue)
		if !ok {
			t.Fatal("change to diskqueue panic")
		}

		if rqDiskQueue.readFileNum != 0 || rqDiskQueue.readPos != 0 {
			t.Fatal("readFileNum != 0 or readPos != 0")
		}

	}

	walRecover.DeleteRepairs()

}

func TestGetForezenQueuesTimeStamps(t *testing.T) {

	l := NewTestLogger(t)
	options := DefaultOption()
	dirPrefix := fmt.Sprintf("TestGetForezenQueuesTimeStamps-%d", time.Now().UnixNano())
	tmpDir, err := ioutil.TempDir("", dirPrefix)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.RemoveAll(tmpDir)
	}()

	options.DataPath = tmpDir
	options.Name = "TestGetForezenQueuesTimeStamps"
	options.MaxBytesPerFile = 32 * 1024
	options.RollTimeSpanSecond = 5

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	ticker := time.NewTicker(12 * time.Second)
	exit := false
	for !exit {
		select {
		case <-ticker.C:
			exit = true
		default:
			err := wal.Put([]byte("a"))
			if err != nil {
				t.Fatal("Put error", err)
			}
		}
	}

	w, ok := wal.(*WALTimeRollQueue)
	if !ok {
		t.Fatal("WALTimeRollQueue type err")
	}

	ts := w.getForezenQueuesTimeStamps()

	if len(ts) != 2 && len(ts) != 3 {
		t.Log("ts len", len(ts))
		t.Fatal("ts len err")
	}

	w.DeleteForezenBefore(time.Now().UnixNano())

	ts = w.getForezenQueuesTimeStamps()

	if len(ts) != 0 {
		t.Fatal("ts len err 2")
	}

	wal.Close()
}

func TestBasicRead(t *testing.T) {
	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestBasicRead-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestBasicRead"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	for i := 0; i < 10; i++ {
		err := wal.Put([]byte("a"))
		if err != nil {
			t.Fatal("Put error", err)
		}
	}
	wal.Close()

	wal2I := NewTimeRollQueue(l, options)

	err = wal2I.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	wal2, _ := wal2I.(*WALTimeRollQueue)

	count := 0

	for {
		msg, ok := wal2.ReadMsg()
		if !ok {
			break
		}
		if string(msg) != "a" {
			t.Fatal("message err")
		}
		count++
	}

	if count != 10 {
		t.Fatal("read count not equal write count")
	}

}

func TestConCurrencyWriteAndRead(t *testing.T) {
	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestConCurrencyWriteAndRead-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestConCurrencyWriteAndRead"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	wg := sync.WaitGroup{}

	for j := 0; j < 2; j++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100000; i++ {
				err := wal.Put([]byte("a"))
				if err != nil {
					t.Fatal("Put error", err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	wal.Close()

	wal2I := NewTimeRollQueue(l, options)
	wal2I.Start()
	count := 0

	wal2, _ := wal2I.(*WALTimeRollQueue)

	for {
		msgBytes, ok := wal2.ReadMsg()
		if ok {
			count++
			if string(msgBytes) != "a" {
				t.Fatal("not equal a")
			}
		} else {
			break
		}
	}

	if count != 200000 {
		t.Fatal("read count not equal write count", "read count", count)
	}
	wal2.Close()
}

// 支持并发读写
func TestConCurrencyWriteAndCurrencyRead(t *testing.T) {

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestConCurrencyWriteAndCurrencyRead-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.SyncTimeout = time.Millisecond
	options.RollTimeSpanSecond = 1
	options.Name = "TestConCurrencyWriteAndCurrencyRead"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	wg := sync.WaitGroup{}

	for j := 0; j < 20; j++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				err := wal.Put([]byte("a"))
				if err != nil {
					t.Fatal("Put error", err)
				}

				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	wal.Close()

	wal2I := NewTimeRollQueue(l, options)
	wal2I.Start()
	count := int32(0)

	wal2, _ := wal2I.(*WALTimeRollQueue)

	for j := 0; j < 5; j++ {
		wg.Add(1)
		go func() {
			for {

				msg, ok := wal2.ReadMsg()

				if !ok {
					break
				} else {
					if string(msg) == "a" {
						atomic.AddInt32(&count, 1)
					}
				}
			}

			wg.Done()
		}()
	}

	wg.Wait()

	if count != 20000 {
		t.Fatal("read count not equal write count", "read count", count)
	}
	wal2.Close()
}

// BenchmarkPut-4   	   78355	     15525 ns/op	     107 B/op	       3 allocs/op
func BenchmarkPut(t *testing.B) {

	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("BenchmarkPut-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "BenchmarkPut"

	wal := NewTimeRollQueue(l, options)
	defer wal.Close()

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	for i := 0; i < t.N; i++ {
		err := wal.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		if err != nil {
			t.Fatal("Put error", err)
		}
	}
}

// per read op:  22784 ns
func TestReadBench(t *testing.T) {

	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("BenchmarkPut-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "BenchmarkPut"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	for i := 0; i < 10000; i++ {
		err := wal.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		if err != nil {
			t.Fatal("Put error", err)
		}
	}

	wal.Close()

	start := time.Now()

	for i := 0; i < 3; i++ {
		wal2I := NewTimeRollQueue(l, options)
		wal2I.Start()

		for i := 0; i < 10000; i++ {

			_, ok := wal2I.ReadMsg()
			if !ok {
				break
			}

		}

		wal2I.Close()

	}

	t.Log("per read op: ", time.Since(start).Nanoseconds()/int64(3*10000), "ns")

}

func TestRepairProcessFunc(t *testing.T) {

	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestRepairProcessFunc-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestRepairProcessFunc"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	for i := 0; i < 100; i++ {
		err := wal.Put([]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
		if err != nil {
			t.Fatal("Put error", err)
		}
	}

	wal.Close()

	rpf := func(msg []byte) bool {
		t.Log("msg: ", string(msg))
		return true
	}

	wal2I := NewTimeRollQueue(l, options)
	wal2I.SetRepairProcessFunc(rpf)
	wal2I.Start()

	for {
		time.Sleep(1 * time.Second)
		if wal2I.FinishRepaired() {
			break
		}
	}

}
