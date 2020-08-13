package diskqueue

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func __init__() {
	rand.Seed(time.Now().UnixNano())
}

const (
	DefaultName            = "timerollqueue"
	DefaultDataPath        = "."
	DefaultMaxBytesPerFile = 100 * 1024 * 1024
	DefaultMinMsgSize      = 32
	DefaultMaxMsgSize      = 4 * 1024 * 1024
	DefaultSyncEvery       = 5000
	DefaultSyncTimeout     = 2 * time.Second
	DefaultRollTimeSpan    = 2 * 3600
	DefaultBackoffDuration = 100 * time.Millisecond

	QueueMetaSuffix = ".diskqueue.meta.dat"
)

type Options struct {
	Name            string        `json:"WALTimeRollQueue.Name"`
	DataPath        string        `json:"WALTimeRollQueue.DataPath"`
	MaxBytesPerFile int64         `json:"WALTimeRollQueue.MaxBytesPerFile"`
	MinMsgSize      int32         `json:"WALTimeRollQueue.MinMsgSize"`
	MaxMsgSize      int32         `json:"WALTimeRollQueue.MaxMsgSize"`
	SyncEvery       int64         `json:"WALTimeRollQueue.SyncEvery"`
	SyncTimeout     time.Duration `json:"WALTimeRollQueue.SyncTimeout"`
	RollTimeSpan    int64         `json:"WALTimeRollQueue.RollTimeSpan"`
	BackoffDuration time.Duration `json:"WALTimeRollQueue.BackoffDuration"`
}

func DefaultOption() *Options {
	return &Options{
		Name:            DefaultName,
		DataPath:        DefaultDataPath,
		MaxBytesPerFile: DefaultMaxBytesPerFile,
		MinMsgSize:      DefaultMinMsgSize,
		MaxMsgSize:      DefaultMaxMsgSize,
		SyncEvery:       DefaultSyncEvery,
		SyncTimeout:     DefaultSyncTimeout,
		RollTimeSpan:    DefaultRollTimeSpan,
		BackoffDuration: DefaultBackoffDuration,
	}
}

// repair process func
type RepairProcessFunc func(msg []byte) bool

type WALTimeRollQueueI interface {
	// 生产消息
	Put([]byte) error
	// 启动， 一旦启动会开启一个新的activeQueue，并且从磁盘中获取所有的队列，将其放入repair列表
	Start() error
	// 读取消息，只会从repair队列中读取消息，不会去读取当前队列的消息
	// 不停地读消息，如果能读到则返回ok，否则返回false
	ReadChan() (<-chan []byte, bool) // this is expected to be an *unbuffered* channel
	// 关闭：关闭activeQueue并且同步磁盘
	Close()
	// 删除某个时间戳之前的冷冻队列
	DeleteForezenBefore(t int64)
	// 删除repair队列
	DeleteRepairs()
}

type WALTimeRollQueue struct {
	// 每次roll的时候要加锁
	sync.RWMutex
	// 当前活跃的队列
	activeQueue Interface
	// 当前冷冻的队列
	forezenQueues []string
	// 当前恢复的队列
	activeRepairQueue Interface
	repairQueueNames  []string
	// instantiation time metadata
	// 队列名字
	Name string
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
	// 滚动的时间间隔，单位为s
	rollTimeSpan int64
	// 下次切换的时间点
	nextRollTime int64
	// 每次调用repair process 函数出错后的回退时间
	backoffDuration time.Duration

	exitChan chan bool
	readExitChan chan bool
	writeExitChan chan bool
	logf AppLogFunc
	rpf  RepairProcessFunc
}

// 排序从大到小
type StringSlice []string

func (s StringSlice) Len() int { return len(s) }
func (s StringSlice) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s StringSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (w *WALTimeRollQueue) getAllRepairQueueNames() ([]string, error) {
	files, err := ioutil.ReadDir(w.dataPath)
	if err != nil {
		return nil, err
	}
	res := []os.FileInfo{}
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), w.Name) && strings.HasSuffix(file.Name(), QueueMetaSuffix) {
			res = append(res, file)
		}
	}

	queueNames := []string{}
	for _, fileInfo := range res {
		queueNames = append(queueNames, strings.TrimSuffix(fileInfo.Name(), QueueMetaSuffix))
	}
	sort.Sort(StringSlice(queueNames))
	return queueNames, nil
}

func (w *WALTimeRollQueue) getNextRepairQueueName(name string) string {
	for _, repairName := range w.repairQueueNames {
		if name < repairName {
			return repairName
		}
	}
	return ""
}

func (w *WALTimeRollQueue) getForezenQueuesTimeStamps() []int {
	times := []int{}
	forezenQueues := []string{}
	w.RLock()
	forezenQueues = append(forezenQueues, w.forezenQueues...)
	w.RUnlock()
	for _, name := range forezenQueues {
		t := strings.TrimPrefix(name, w.Name+"_")
		time, err := strconv.Atoi(t)
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue getForezenQueuesTimeStamps strconv.Atoi %s", err)
		} else {
			times = append(times, time)
		}
	}
	return times
}

func (w *WALTimeRollQueue) resetQueue(name string) error {
	q := New(name, w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	if q != nil {
		return q.ResetReadMetaData()
	}
	return nil
}

func (w *WALTimeRollQueue) delteQueue(name string) error {
	q := New(name, w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	if q != nil {
		return q.Empty()
	}
	return nil
}

func (w *WALTimeRollQueue) GetNowActiveQueueName() string {
	return w.Name + "_" + strconv.Itoa(int(time.Now().Unix()/w.rollTimeSpan*w.rollTimeSpan))
}

func (w *WALTimeRollQueue) GetNextRollTime() int64 {
	return time.Now().Unix()/w.rollTimeSpan*w.rollTimeSpan + w.rollTimeSpan
}

func (w *WALTimeRollQueue) Init() error {
	// 每次启动的时候用当前最新时间
	w.activeQueue = New(w.Name+"_"+strconv.Itoa(int(time.Now().Unix())), w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	w.nextRollTime = w.GetNextRollTime()
	w.forezenQueues = []string{}
	var err error
	w.repairQueueNames, err = w.getAllRepairQueueNames()
	if len(w.repairQueueNames) > 0 {
		w.activeRepairQueue = New(w.repairQueueNames[0], w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	}
	if err != nil {
		return err
	}
	//刷新所有repair队列的元信息中的readPos， readNum
	w.ResetRepairs()
	return nil

}

// 并发安全的，多个进程同时发起Roll，那么只有一个会成功
func (w *WALTimeRollQueue) Roll() {

	w.Lock()
	defer w.Unlock()

	newQueueName := w.GetNowActiveQueueName()
	if newQueueName == w.activeQueue.GetName() {
		return
	}

	newQueue := New(newQueueName, w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	err := w.activeQueue.Close()
	if err != nil {
		w.logf(ERROR, "WALTimeRollQueue(%s) failed to Close - %s", w.activeQueue.GetName(), err.Error())
	} else {
		w.logf(INFO, "WALTimeRollQueue(%s) success to Close", w.activeQueue.GetName())
	}
	w.forezenQueues = append(w.forezenQueues, w.activeQueue.GetName())
	w.activeQueue = newQueue
}

func (w *WALTimeRollQueue) Put(msg []byte) error {

	select {
	case <-w.exitChan:
		return errors.New("WALTimeRollQueue exit...")
	default:
		if time.Now().Unix() > w.nextRollTime {
			// 随机睡眠一下，防止大家共同进入加锁
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(500)))
			w.Roll()
		}
		w.Lock()
		defer w.Unlock()
		return w.activeQueue.Put(msg)
	}

}

func (w *WALTimeRollQueue) Start() error {
	err := w.Init()
	if err != nil {
		return err
	}
	// 开启一个读go程去从repair队列中将读出来，等待上层取走，一旦读通道空出来则开始读，第一次读的时候去返回一个空的消息
	go func() {

		for {
			select {
			case <- w.exitChan:
				close(w.readExitChan)
				return
			default:
				msgChan, ok := w.ReadChan()
				if !ok {
					w.logf(INFO, "WALTimeRollQueue repair finish...")
					w.DeleteRepairs()
					w.logf(INFO, "WALTimeRollQueue delete repair queues")
					close(w.readExitChan)
					return
				}else {
					if msgChan == nil {
						continue
					}else {
						msg := <- msgChan
						for {
							ok := w.rpf(msg)
							if !ok {
								w.logf(ERROR, "WALTimeRollQueue process repair message failed")
								time.Sleep(w.backoffDuration)
							}
						}
					}
					
				}
			}
		}
	}()

	return nil
}

func (w *WALTimeRollQueue) ReadChan() (<-chan []byte, bool) {

	select {
	case <-w.exitChan:
		return nil, true
	default:
		goto DO
	}

DO:
	if w.activeRepairQueue == nil {
		return nil, false
	} else {
		if w.activeRepairQueue.JudgeNoMessage() {
			// 切换下一个队列
			w.activeRepairQueue.Close()
			newRepairQueue := w.getNextRepairQueueName(w.activeRepairQueue.GetName())
			if newRepairQueue == "" {
				return nil, false
			}
			w.activeRepairQueue = New(newRepairQueue, w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
			return w.ReadChan()
		}

		return w.activeRepairQueue.ReadChan(), true
	}

}

func (w *WALTimeRollQueue) CloseWrite() error {
	w.Lock()
	defer w.Unlock()
	return w.activeQueue.Close()
}

func (w *WALTimeRollQueue) Close() {
	w.Lock()
	defer w.Unlock()
	// 发送退出信号
	w.exitChan <- true
	// 等待读退出
	<- w.readExitChan
	if w.activeRepairQueue != nil {
		err := w.activeRepairQueue.Close()
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue Close activeRepairQueue err %s", err)
		}
	}

	err := w.CloseWrite()
	if err != nil {
		w.logf(ERROR, "WALTimeRollQueue CloseWrite %s", err)
	}

}
func (w *WALTimeRollQueue) DeleteForezenBefore(t int64) {
	forezenTimes := w.getForezenQueuesTimeStamps()
	for _, time := range forezenTimes {
		if time <= int(t) {
			err := w.delteQueue(w.Name + "_" + strconv.Itoa(time))
			if err != nil {
				w.logf(ERROR, "WALTimeRollQueue DeleteForezenBefore delteQueue %s", err)
			}
		}
	}
}

func (w *WALTimeRollQueue) ResetRepairs() {
	for _, name := range w.repairQueueNames {
		err := w.resetQueue(name)
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue ResetRepairs resetQueue %s", err)
		}
	}
}

func (w *WALTimeRollQueue) DeleteRepairs() {
	for _, name := range w.repairQueueNames {
		err := w.delteQueue(name)
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue DeleteRepairs delteQueue %s", err)
		}
	}
}

func NewTimeRollQueue(log AppLogFunc, options Options) WALTimeRollQueueI {

	return &WALTimeRollQueue{
		Name:            options.Name,
		dataPath:        options.DataPath,
		maxBytesPerFile: options.MaxBytesPerFile,
		minMsgSize:      options.MinMsgSize,
		maxMsgSize:      options.MinMsgSize,
		syncEvery:       options.SyncEvery,
		syncTimeout:     options.SyncTimeout,
		rollTimeSpan:    options.RollTimeSpan,
		backoffDuration: options.BackoffDuration,
		logf:            log,

		exitChan: make(chan bool),
		writeExitChan: make(chan bool),
		readExitChan: make(chan bool),
	}

}
