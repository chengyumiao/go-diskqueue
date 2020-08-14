package diskqueue

import (
	"fmt"
	"io/ioutil"
	"os"
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
	dirPrefix := fmt.Sprintf("TestGetAllRepairQueueNames-%d", time.Now().UnixNano())
	tmpDir, err := ioutil.TempDir("", dirPrefix)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestGetAllRepairQueueNames"
	options.MaxBytesPerFile = 32 * 1024
	options.RollTimeSpan = 5

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
		rollTimeSpan:    options.RollTimeSpan,
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

	walRecover.ResetRepairs()

	for _, repairQueue := range walRecover.repairQueueNames {

		rq := New(repairQueue, walRecover.dataPath, walRecover.maxBytesPerFile, walRecover.minMsgSize, walRecover.maxMsgSize, walRecover.syncEvery, walRecover.syncTimeout, walRecover.logf)
		rqDiskQueue, ok := rq.(*diskQueue)
		if !ok {
			t.Fatal("change to diskqueue panic")
		}
		rqDiskQueue.readFileNum = rqDiskQueue.writeFileNum
		rqDiskQueue.readPos = rqDiskQueue.writePos
		rqDiskQueue.persistMetaData()
	}

	walRecover.DeleteRepairs()

}

// 需要测试的函数
// func (w *WALTimeRollQueue) getForezenQueuesTimeStamps() []int {
// func (w *WALTimeRollQueue) resetQueue(name string) error {
// func (w *WALTimeRollQueue) delteQueue(name string) error {
// func (w *WALTimeRollQueue) GetNowActiveQueueName() string {
// func (w *WALTimeRollQueue) GetNextRollTime() int64 {
// func (w *WALTimeRollQueue) Roll() {
// func (w *WALTimeRollQueue) Put(msg []byte) error {
// func (w *WALTimeRollQueue) Start() error {
// func (w *WALTimeRollQueue) ReadChan() (<-chan []byte, bool) {
// func (w *WALTimeRollQueue) CloseWrite() error {
// func (w *WALTimeRollQueue) Close() {
// func (w *WALTimeRollQueue) DeleteForezenBefore(t int64) {
