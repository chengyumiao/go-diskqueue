package diskqueue

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
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
	options.RollTimeSpanSecond = 2
	options.RotationTimeSecond = 12

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}
	sleepSecond := 16
	ticker := time.NewTicker(time.Duration(sleepSecond) * time.Second)
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

	start := time.Now()
	wal.Close()
	fmt.Println("Spend: ", time.Since(start).Seconds())

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

	repairNames, err := walRecover.getAllQueueNames()
	if err != nil {
		t.Fatal("getAllRepairQueueNames err ", err)
	}
	if len(repairNames) <= 0 {
		t.Fatal("repair names len err")
	}

	if !(len(repairNames) == sleepSecond/int(options.RollTimeSpanSecond)+1 || len(repairNames) == sleepSecond/int(options.RollTimeSpanSecond)+2) {
		t.Fatal("repairNames err")
	}

	walRecover.init()

	nextQueue := walRecover.getNextRepairQueueName(walRecover.GetRepairQueueNames()[0])
	if len(walRecover.GetRepairQueueNames()) > 1 && walRecover.GetRepairQueueNames()[1] != nextQueue {
		// fmt.Println(walRecover.GetRepairQueueNames())
		// fmt.Println(nextQueue)
		t.Fatal("getNextRepairQueueName is err")
	}

	if len(walRecover.GetRepairQueueNames()) > 3 {
		nextQueue := walRecover.getNextRepairQueueName(walRecover.GetRepairQueueNames()[1])
		if nextQueue != walRecover.GetRepairQueueNames()[2] {
			t.Fatal(" get next 2 err")
		}
	}

	if len(walRecover.GetRepairQueueNames()) == 1 && nextQueue != "" {
		// fmt.Println(repairNames)
		// fmt.Println(nextQueue)
		t.Fatal("getNextRepairQueueName is err")
	}

	for _, repairQueue := range walRecover.GetRepairQueueNames() {

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

	for _, repairQueue := range walRecover.GetRepairQueueNames() {

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

	for _, repairQueue := range walRecover.GetRepairQueueNames() {

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

func TestDeleteForezenQueues(t *testing.T) {
	l := NewTestLogger(t)
	options := DefaultOption()
	dirPrefix := fmt.Sprintf("TestDeleteForezenQueues-%d", time.Now().UnixNano())
	tmpDir, err := ioutil.TempDir("", dirPrefix)
	if err != nil {
		panic(err)
	}
	defer func() {
		os.RemoveAll(tmpDir)
	}()

	options.DataPath = tmpDir
	options.Name = "TestDeleteForezenQueues"
	options.MaxBytesPerFile = 32 * 1024
	options.RollTimeSpanSecond = 2

	walI := NewTimeRollQueue(l, options)

	err = walI.Start()
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
			err := walI.Put([]byte("a"))
			if err != nil {
				t.Fatal("Put error", err)
			}
		}
	}
	wal, _ := walI.(*WALTimeRollQueue)

	ts := wal.getForezenQueuesTimeStamps()

	start := time.Now()

	if len(ts) < 5 {
		t.Fatal("TestDeleteForezenQueues len err")
	}

	wal.DeleteForezenBefore(start.Unix())

	names, err := wal.getAllQueueNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) > 1 {
		t.Fatal("TestDeleteForezenQueues names len err")
	}

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

func TestRotation(t *testing.T) {

	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestRotation-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.SyncTimeout = time.Second
	options.RollTimeSpanSecond = 1
	options.RotationTimeSecond = 3
	options.Name = "TestRotation"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	wal2, _ := wal.(*WALTimeRollQueue)

	writeErr := make(chan error)
	readErr := make(chan error)

	// write go
	go func() {

		ticker := time.NewTicker(100 * time.Millisecond)

	LOOP:
		for {

			select {
			case err := <-readErr:
				writeErr <- err
				break LOOP
			case <-ticker.C:
				err := wal.Put([]byte("a"))
				if err != nil {
					t.Log("Put error", err)
					writeErr <- err
					break LOOP
				}

			}

		}
	}()

	go func() {

		for i := 0; i < 3; i++ {
			time.Sleep(4 * time.Second)
			names, err := wal2.getAllQueueNames()
			if err != nil {
				t.Log("getAllRepairQueueNames err")
				readErr <- err
				return
			}
			if len(names) <= 3 {
				t.Log("len err", len(names), " ", names)
				readErr <- errors.New("len err <= 3 " + strconv.Itoa(len(names)))
				return
			} else {
				t.Log("begin rm len: ", len(names), " ", names)

			}

			wal2.deleteExpired()
			names, err = wal2.getAllQueueNames()
			if err != nil {
				t.Log("getAllRepairQueueNames err")
				readErr <- err
				return
			}
			if len(names) <= 3 {

				t.Log("after rm len: ", len(names), " ", names)

			} else {
				t.Log("len err", len(names), " ", names)
				readErr <- errors.New("len err" + strconv.Itoa(len(names)))
				return
			}

		}

		readErr <- nil
	}()

	err = <-writeErr
	if err != nil {
		t.Fatal("ERR: ", err)
	}

	t.Log("get exit")

	defer wal.Close()

}

func TestDelFinishRepair(t *testing.T) {
	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("TestRotation-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.SyncTimeout = time.Second
	options.RollTimeSpanSecond = 1
	options.RotationTimeSecond = 3
	options.Name = "TestRotation"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	writeErr := make(chan error)
	readErr := make(chan error)

	// write go
	go func() {

		ticker := time.NewTicker(100 * time.Millisecond)

	LOOP:
		for {

			select {
			case err := <-readErr:
				writeErr <- err
				break LOOP
			case <-ticker.C:
				err := wal.Put([]byte("a"))
				if err != nil {
					t.Log("Put error", err)
					writeErr <- err
					break LOOP
				}

			}

		}
	}()

	time.Sleep(10 * time.Second)
	wal.Close()
	time.Sleep(1 * time.Second)

	wal = NewTimeRollQueue(l, options)
	err = wal.Start()
	if err != nil {
		t.Fatal("wal2 start err", err)
	}
	wal2 := wal.(*WALTimeRollQueue)

	names, err := wal2.getAllQueueNames()
	if err != nil {
		t.Log("getAllRepairQueueNames err")
		readErr <- err
		return
	}
	wal2.activeRepairQueue.Close()

	wal2.activeRepairQueue = New(names[len(names)-1], options.DataPath, options.MaxBytesPerFile, options.MinMsgSize, options.MaxMsgSize, options.SyncEvery, options.SyncTimeout, wal2.logf)
	wal2.DeleteRepairs()

	names, err = wal2.getAllQueueNames()
	if err != nil {
		t.Fatal(err)
	}

	if len(names) != 1 {
		t.Fatal("len names err")
	}
}

func TestCopy(t *testing.T) {
	a := []int{0, 1, 2}
	b := make([]int, 3, len(a))
	copy(a, b)
	if b[2] != a[2] {
		t.Fatal("err")
	}
}
