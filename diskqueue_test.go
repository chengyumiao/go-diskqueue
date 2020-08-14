package diskqueue

import (
	"bytes"
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Equal(t *testing.T, expected, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   %#v (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func NotEqual(t *testing.T, expected, actual interface{}) {
	if reflect.DeepEqual(expected, actual) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, expected, actual)
		t.FailNow()
	}
}

func Nil(t *testing.T, object interface{}) {
	if !isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\t   <nil> (expected)\n\n\t!= %#v (actual)\033[39m\n\n",
			filepath.Base(file), line, object)
		t.FailNow()
	}
}

func NotNil(t *testing.T, object interface{}) {
	if isNil(object) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tExpected value not to be <nil>\033[39m\n\n",
			filepath.Base(file), line)
		t.FailNow()
	}
}

func isNil(object interface{}) bool {
	if object == nil {
		return true
	}

	value := reflect.ValueOf(object)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice && value.IsNil() {
		return true
	}

	return false
}

type tbLog interface {
	Log(...interface{})
}

func NewTestLogger(tbl tbLog) AppLogFunc {
	return func(lvl LogLevel, f string, args ...interface{}) {
		tbl.Log(fmt.Sprintf(lvl.String()+": "+f, args...))
	}
}

func TestDiskQueue(t *testing.T) {
	l := NewTestLogger(t)

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1024, 4, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)

	msg := []byte("test")
	err = dq.Put(msg)
	Nil(t, err)

	msgOut := <-dq.ReadChan()
	Equal(t, msg, msgOut)
}

func TestDiskQueueRoll(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	ml := int64(len(msg))
	dq := New(dqName, tmpDir, 10*(ml+4), int32(ml), 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)

	for i := 0; i < 10; i++ {
		err := dq.Put(msg)
		Nil(t, err)
	}

	Equal(t, int64(1), dq.(*diskQueue).writeFileNum)
	Equal(t, int64(0), dq.(*diskQueue).writePos)
}

func assertFileNotExist(t *testing.T, fn string) {
	f, err := os.OpenFile(fn, os.O_RDONLY, 0600)
	Equal(t, (*os.File)(nil), f)
	Equal(t, true, os.IsNotExist(err))
}

func TestDiskQueueEmpty(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	dq := New(dqName, tmpDir, 100, 0, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		Nil(t, err)
	}

	for i := 0; i < 3; i++ {
		<-dq.ReadChan()
	}

	numFiles := dq.(*diskQueue).writeFileNum
	dq.Empty()

	assertFileNotExist(t, dq.(*diskQueue).metaDataFileName())
	for i := int64(0); i <= numFiles; i++ {
		assertFileNotExist(t, dq.(*diskQueue).fileName(i))
	}
	Equal(t, dq.(*diskQueue).writeFileNum, dq.(*diskQueue).readFileNum)
	Equal(t, dq.(*diskQueue).writePos, dq.(*diskQueue).readPos)
	Equal(t, dq.(*diskQueue).readPos, dq.(*diskQueue).nextReadPos)
	Equal(t, dq.(*diskQueue).readFileNum, dq.(*diskQueue).nextReadFileNum)

	for i := 0; i < 100; i++ {
		err := dq.Put(msg)
		Nil(t, err)
	}

	for i := 0; i < 100; i++ {
		<-dq.ReadChan()
	}

	Equal(t, dq.(*diskQueue).writeFileNum, dq.(*diskQueue).readFileNum)
	time.Sleep(3 * time.Second)
	Equal(t, dq.(*diskQueue).writePos, dq.(*diskQueue).readPos)
	Equal(t, dq.(*diskQueue).readPos, dq.(*diskQueue).nextReadPos)
}

func TestDiskQueueCorruption(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_corruption" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	// require a non-zero message length for the corrupt (len 0) test below
	dq := New(dqName, tmpDir, 1000, 10, 1<<10, 5, 2*time.Second, l)
	defer dq.Close()

	msg := make([]byte, 123) // 127 bytes per message, 8 (1016 bytes) messages per file
	for i := 0; i < 25; i++ {
		dq.Put(msg)
	}

	// corrupt the 2nd file
	dqFn := dq.(*diskQueue).fileName(1)
	os.Truncate(dqFn, 500) // 3 valid messages, 5 corrupted

	for i := 0; i < 19; i++ { // 1 message leftover in 4th file
		Equal(t, msg, <-dq.ReadChan())
	}

	// corrupt the 4th (current) file
	dqFn = dq.(*diskQueue).fileName(3)
	os.Truncate(dqFn, 100)

	dq.Put(msg) // in 5th file

	Equal(t, msg, <-dq.ReadChan())

	// write a corrupt (len 0) message at the 5th (current) file
	dq.(*diskQueue).writeFile.Write([]byte{0, 0, 0, 0})

	// force a new 6th file - put into 5th, then readOne errors, then put into 6th
	dq.Put(msg)
	dq.Put(msg)

	Equal(t, msg, <-dq.ReadChan())
}

type md struct {
	readFileNum  int64
	writeFileNum int64
	readPos      int64
	writePos     int64
}

func readMetaDataFile(fileName string, retried int) md {
	f, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		// provide a simple retry that results in up to
		// another 500ms for the file to be written.
		if retried < 9 {
			retried++
			time.Sleep(50 * time.Millisecond)
			return readMetaDataFile(fileName, retried)
		}
		panic(err)
	}
	defer f.Close()

	var ret md
	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n",
		&ret.readFileNum, &ret.readPos,
		&ret.writeFileNum, &ret.writePos)
	if err != nil {
		panic(err)
	}
	return ret
}

func TestDiskQueueSyncAfterRead(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_disk_queue_read_after_sync" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 1<<11, 0, 1<<10, 2500, 50*time.Millisecond, l)
	defer dq.Close()

	msg := make([]byte, 1000)
	dq.Put(msg)

	for i := 0; i < 10; i++ {
		d := readMetaDataFile(dq.(*diskQueue).metaDataFileName(), 0)
		if d.readFileNum == 0 &&
			d.writeFileNum == 0 &&
			d.readPos == 0 &&
			d.writePos == 1004 {
			// success
			goto next
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("fail")

next:
	dq.Put(msg)
	<-dq.ReadChan()

	for i := 0; i < 10; i++ {
		d := readMetaDataFile(dq.(*diskQueue).metaDataFileName(), 0)
		if d.readFileNum == 0 &&
			d.writeFileNum == 0 &&
			d.readPos == 1004 &&
			d.writePos == 2008 {
			// success
			goto done
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("fail")

done:
}

func TestDiskQueueTorture(t *testing.T) {
	var wg sync.WaitGroup

	l := NewTestLogger(t)
	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := New(dqName, tmpDir, 262144, 0, 1<<10, 2500, 2*time.Second, l)
	NotNil(t, dq)
	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")

	numWriters := 4
	numReaders := 4
	readExitChan := make(chan int)
	writeExitChan := make(chan int)

	var count int64
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case <-writeExitChan:
					return
				default:
					err := dq.Put(msg)
					if err == nil {
						atomic.AddInt64(&count, 1)
					}
				}
			}
		}()
	}

	time.Sleep(1 * time.Second)

	dq.Close()

	t.Logf("closing writeExitChan")
	close(writeExitChan)
	wg.Wait()

	t.Logf("restarting diskqueue")

	dq = New(dqName, tmpDir, 262144, 0, 1<<10, 2500, 2*time.Second, l)
	defer dq.Close()
	NotNil(t, dq)

	var readCount int64
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case m := <-dq.ReadChan():
					Equal(t, m, msg)
					atomic.AddInt64(&readCount, 1)
				case <-readExitChan:
					return
				}
			}
		}()
	}

	t.Logf("waiting for depth 0")
	t.Logf("closing readExitChan")
	close(readExitChan)
	wg.Wait()
}
