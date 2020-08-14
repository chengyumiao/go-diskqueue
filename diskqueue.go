package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	GetName() string
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	// 判断是否已经读完当前队列里面的消息
	ReadEnd() bool
	// 清空某个队列相关所有的相关信息
	Empty() error
	ResetReadMetaData() error
}

// diskQueue implements a filesystem backed FIFO queue
// 基于文件系统的FIFO队列
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	// 读写指针位置
	readPos  int64
	writePos int64
	// 读写文件数量
	readFileNum  int64
	writeFileNum int64
	sync.RWMutex

	// instantiation time metadata
	// 队列名字
	name string
	// 队列路径
	dataPath string
	// 每个文件的最大大小
	maxBytesPerFile int64 // currently this cannot change once created
	// 最小消息大小
	minMsgSize int32
	// 最大消息大小
	maxMsgSize int32
	// 每多少次写同步一次
	syncEvery int64 // number of writes per fsync
	// 最迟的同步时间，如果一段时间没有同步，则开启同步
	syncTimeout time.Duration // duration of time per fsync
	// 退出标识位
	exitFlag int32
	// 是否需要同步
	needSync bool

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	// 下个读取的位置
	nextReadPos int64
	// 下次读取的文件数量
	nextReadFileNum int64
	// 读文件标识符
	readFile *os.File
	// 写文件表示符
	writeFile *os.File
	// 读缓冲
	reader *bufio.Reader
	// 写缓冲
	writeBuf bytes.Buffer

	// exposed via ReadChan()
	// 通过读chan向外暴露读
	readChan chan []byte

	// 写通道
	writeChan chan []byte
	// 写通道回应
	writeResponseChan chan error

	// 是否还有消息要读
	noMessageLock sync.RWMutex
	noMessage     chan bool

	emptyChan         chan int
	emptyResponseChan chan error
	// 退出通道
	exitChan chan int
	// 退出同步通道
	exitSyncChan chan int
	// 日志输出函数
	logf AppLogFunc
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
// 创建磁盘队列
// 参数：队列名，数据路径，每个文件最大字节数，消息消息大小，最大消息大小，每多少个请求同步一次，同步耗时
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		noMessage:         make(chan bool, 1),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	// 读取元数据
	err := d.retrieveMetaData()
	// 如果不为空，或者不存在则报错
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	// 启动循环
	go d.ioLoop()
	return &d
}
func (d *diskQueue) GetName() string {
	return d.name
}

// ReadChan returns the receive-only []byte channel for reading data
// 返回只读型通道
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
// 想写通道中写数据，从写回应通道里面获取返回信号
func (d *diskQueue) Put(data []byte) error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	err := <-d.writeResponseChan
	if err != nil {
		return err
	} else {
		d.NoMessageSetFalse()
		return nil
	}
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	// 退出
	err := d.exit()
	if err != nil {
		return err
	}
	// 同步
	return d.sync()
}

// 退出
func (d *diskQueue) exit() error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1
	// 关闭退出通道
	close(d.exitChan)
	// ensure that ioLoop has exited
	// 等待同步完成通道
	<-d.exitSyncChan

	// 关闭读文件描述符
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	// 关闭写文件描述符
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	// 向清空信号中发送，等待回复
	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueue) NoMessageSetFalse() {
	d.noMessageLock.Lock()
	defer d.noMessageLock.Unlock()
	// 不管有没有，都置为false
	select {
	case <-d.noMessage:
		d.noMessage <- false
	default:
		d.noMessage <- false
	}
}

func (d *diskQueue) NoMessageSetTrue() {
	d.noMessageLock.Lock()
	defer d.noMessageLock.Unlock()
	// 不管有没有，都置为true
	select {
	case <-d.noMessage:
		d.noMessage <- true
	default:
		d.noMessage <- true
	}
}

func (d *diskQueue) ReadEnd() bool {
	d.noMessageLock.RLock()
	defer d.noMessageLock.RUnlock()
	select {
	case ok := <-d.noMessage:
		return ok
	default:
		return false
	}
}

func (d *diskQueue) deleteAllFiles() error {
	// 将写文件号与读文件号对齐，并且指向最上面
	err := d.skipToNextRWFile()
	// 删除元文件
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

// 跳过所有没有读的文件，并删除这些需要读的文件，直接将readFileNum 到 writeFileNum 对齐到 writeFileNum下一个。
// 如果不希望滚动，那么要自己控制这个函数
func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := int64(0); i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		// 删除对应的文件
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
// 支持滚动读，一旦发现读取的文件已经要大于当前文件的大小，则开启下一个读
// 如果读取报错，说该文件不存在，则会返回文件不存在的报错
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
// 往文件中追加写
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes

	if d.writePos >= d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
// 将当前的文件缓冲区同步，并且关闭写
// 持久化元信息
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		// 文件描述符自带的方法
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
// 恢复元数据信息
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	// 打开元文件
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 读取元文件中的信息
	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n",
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// 将元文件的读num，与读位置都设为从头开始
func (d *diskQueue) ResetReadMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	// 打开元文件
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 读取元文件中的信息
	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n",
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	d.readFileNum = 0
	d.readPos = 0
	return d.persistMetaData()
}

// persistMetaData atomically writes state to the filesystem
// 持久化元数据自动写状态给文件系统
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// 每次重启的时候需要将readFileNum，readPos都置为0
	_, err = fmt.Fprintf(f, "%d,%d\n%d,%d\n",
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	// 将临时的文件改为正式文件
	return os.Rename(tmpFileName, fileName)
}

// 获取队列对应元信息文件名（  队列名.diskqueue.meta.dat ）
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

// 每个队列名字会生成多个不同fileNum的磁盘队列
// 这个应该是与具体的磁盘文件不能太大有关系
// 每次队列最终的物理存储的文件名字为：       队列名.diskqueue.$fileNum.dat
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true
	}
}

// 处理错误，将原先的文件名字，改为 名字.bad
func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	// 切换一个新的队列去读取

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
// ioLoop：用来向外暴露一个读的通道，可以支持多个并发的队列消费者。
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		// 达到一定次数就开始刷盘
		if count == d.syncEvery {
			d.needSync = true
		}

		// 一旦开始刷盘，那么久开始
		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil

			d.NoMessageSetTrue()

		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			count++
			// moveForward sets needSync flag if a file is removed
			// 如果读取的时候滚动了文件，会删除相关的文件
			d.moveForward()
		// 收到清空的信号，则删除元文件，以及相关文件所有的数据文件，删除的消息赋给清空回复信号
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		// 写通道里面收到写信号，开始写动作，并将回应发送给写回应通道里面
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		// 定期去看看是否需要同步
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		// 收到退出信号，则退出
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
