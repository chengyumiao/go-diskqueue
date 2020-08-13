package diskqueue

import (
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultName            = "timerollqueue"
	DefaultDataPath        = "."
	DefaultMaxBytesPerFile = 100 * 1024 * 1024
	DefaultMinMsgSize      = 32
	DefaultMaxMsgSize      = 4 * 1024 * 1024
	DefaultSyncEvery       = 5000
	DefaultSyncTimeout     = 2 * time.Second
	DefaultRollTimeSpan    = 2 * 3600

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
	}
}

type WALTimeRollQueueI interface {
	// 生产消息
	Put([]byte) error
	// 启动， 一旦启动会开启一个新的activeQueue，并且从磁盘中获取所有的队列，将其放入repair列表
	Start() error
	// 读取消息，只会从repair队列中读取消息，不会去读取当前队列的消息
	// 自动遍历所有的repair queues，知道所有的队列被读完了，则关闭此通道
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	// 关闭：关闭activeQueue并且同步磁盘
	Close() error
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
	repairQueueNames []string
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

	logf AppLogFunc
}

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

	return queueNames, nil
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
			w.logf(ERROR, "getForezenQueuesTimeStamps strconv.Atoi %s", err)
		} else {
			times = append(times, time)
		}
	}
	return times
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

func (w *WALTimeRollQueue) CreateNowActiveQueue() Interface {
	return New(w.GetNowActiveQueueName(), w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
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
	return err
}

func (w *WALTimeRollQueue) Roll() {

	w.Lock()
	defer w.Unlock()
	newQueue := w.CreateNowActiveQueue()
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
	if time.Now().Unix() > w.nextRollTime {
		w.Roll()
	}
	w.Lock()
	defer w.Unlock()
	return w.activeQueue.Put(msg)
}

func (w *WALTimeRollQueue) Start() error {
	err := w.Init()
	if err != nil {
		return err
	}
	// todo: 开启一个读go程去从repair队列中将消息一条一条地读到Readchan中
	return nil
}
func (w *WALTimeRollQueue) ReadChan() <-chan []byte {
	return nil
}
func (w *WALTimeRollQueue) Close() error {
	w.Lock()
	defer w.Unlock()
	w.activeQueue.Close()
	// todo: 关闭后端循环读go程
	return nil
}
func (w *WALTimeRollQueue) DeleteForezenBefore(t int64) {
	forezenTimes := w.getForezenQueuesTimeStamps()
	for _, time := range forezenTimes {
		if time <= int(t) {
			err := w.delteQueue(w.Name + "_" + strconv.Itoa(time))
			if err != nil {
				w.logf(ERROR, "DeleteForezenBefore delteQueue %s", err)
			}
		}
	}
}
func (w *WALTimeRollQueue) DeleteRepairs() {
	for _, name := range w.repairQueueNames {
		err := w.delteQueue(name)
		if err != nil {
			w.logf(ERROR, "DeleteRepairs delteQueue %s", err)
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
		logf:            log,
	}

}
