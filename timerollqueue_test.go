package diskqueue

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

// 基本的写入读取的测试
func TestBasicWriteRead(t *testing.T) {
	l := NewTestLogger(t)
	options := DefaultOption()

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	options.DataPath = tmpDir
	options.Name = "TestBasicWriteRead"

	wal := NewTimeRollQueue(l, options)

	err = wal.Start()
	if err != nil {
		t.Fatal("start err", err)
	}

	for i := 0; i < 10; i++ {
		wal.Put([]byte("a"))
	}

	wal.Close()

	wal = NewTimeRollQueue(l, options)
	msgChan, ok := wal.readChan()
	if ok {
		msgBytes := <-msgChan
		fmt.Println(string(msgBytes))
	}
}

// 需要测试的函数
// func (w *WALTimeRollQueue) getAllRepairQueueNames() ([]string, error) {
// func (w *WALTimeRollQueue) getNextRepairQueueName(name string) string {
// func (w *WALTimeRollQueue) getForezenQueuesTimeStamps() []int {
// func (w *WALTimeRollQueue) resetQueue(name string) error {
// func (w *WALTimeRollQueue) delteQueue(name string) error {
// func (w *WALTimeRollQueue) GetNowActiveQueueName() string {
// func (w *WALTimeRollQueue) GetNextRollTime() int64 {
// func (w *WALTimeRollQueue) Init() error {
// func (w *WALTimeRollQueue) Roll() {
// func (w *WALTimeRollQueue) Put(msg []byte) error {
// func (w *WALTimeRollQueue) Start() error {
// func (w *WALTimeRollQueue) ReadChan() (<-chan []byte, bool) {
// func (w *WALTimeRollQueue) CloseWrite() error {
// func (w *WALTimeRollQueue) Close() {
// func (w *WALTimeRollQueue) DeleteForezenBefore(t int64) {
// func (w *WALTimeRollQueue) ResetRepairs() {
// func (w *WALTimeRollQueue) DeleteRepairs() {
