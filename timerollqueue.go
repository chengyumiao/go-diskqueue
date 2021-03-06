package diskqueue

import (
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func __init__() {
	rand.Seed(time.Now().UnixNano())
}

const (
	DefaultName               = "timerollqueue"
	DefaultDataPath           = "."
	DefaultMaxBytesPerFile    = 100 * 1024 * 1024
	DefaultMinMsgSize         = 0
	DefaultMaxMsgSize         = 4 * 1024 * 1024
	DefaultSyncEvery          = 500
	DefaultSyncTimeout        = 2 * time.Second
	DefaultRollTimeSpanSecond = 2 * 3600
	DefaultRotationTimeSecond = 4 * 3600
	DefaultBackoffDuration    = 100 * time.Millisecond
	DefaultLimiterBatch       = 100000
	DefaultLimiterDuration    = 0 // 默认不去做流控了，实在需要在应用层自己设置呗
	QueueMetaSuffix           = ".diskqueue.meta.dat"
)

type Options struct {
	Name               string        `json:"WALTimeRollQueue.Name"`
	DataPath           string        `json:"WALTimeRollQueue.DataPath"`
	MaxBytesPerFile    int64         `json:"WALTimeRollQueue.MaxBytesPerFile"`
	MinMsgSize         int32         `json:"WALTimeRollQueue.MinMsgSize"`
	MaxMsgSize         int32         `json:"WALTimeRollQueue.MaxMsgSize"`
	SyncEvery          int64         `json:"WALTimeRollQueue.SyncEvery"`
	SyncTimeout        time.Duration `json:"WALTimeRollQueue.SyncTimeout"`
	RollTimeSpanSecond int64         `json:"WALTimeRollQueue.RollTimeSpanSecond"` // 配置单位为s
	RotationTimeSecond int64         `json:"WALTimeRollQueue.RotationTimeSecond"` // 单位为s
	BackoffDuration    time.Duration `json:"WALTimeRollQueue.BackoffDuration"`
	LimiterBatch       int           `json:"WALTimeRollQueue.LimiterBatch"`
	LimiterDuration    time.Duration `json:"WALTimeRollQueue.LimiterDuration"`
}

func DefaultOption() *Options {
	return &Options{
		Name:               DefaultName,
		DataPath:           DefaultDataPath,
		MaxBytesPerFile:    DefaultMaxBytesPerFile,
		MinMsgSize:         DefaultMinMsgSize,
		MaxMsgSize:         DefaultMaxMsgSize,
		SyncEvery:          DefaultSyncEvery,
		SyncTimeout:        DefaultSyncTimeout,
		RollTimeSpanSecond: DefaultRollTimeSpanSecond,
		RotationTimeSecond: DefaultRotationTimeSecond,
		BackoffDuration:    DefaultBackoffDuration,
		LimiterBatch:       DefaultLimiterBatch,
		LimiterDuration:    DefaultLimiterDuration,
	}
}

// repair process func
type RepairProcessFunc func(msg []byte) bool

type WALTimeRollQueueI interface {
	// 生产消息
	Put([]byte) error
	// 启动， 一旦启动会开启一个新的activeQueue，并且从磁盘中获取所有的队列，将其放入repair列表
	Start() error

	ReadMsg() ([]byte, bool)
	// 关闭：关闭activeQueue并且同步磁盘
	Close()

	Stats() *WALTimeRollQueueStats

	SetRepairProcessFunc(rpf func(msg []byte) bool)
	// 是否修复完成
	FinishRepaired() bool
	// 删除某个时间戳之前的冷冻队列
	DeleteForezenBefore(t int64)
	// 删除repair队列
	DeleteRepairs()
}

type WALTimeRollQueueStats struct {
	Name                   string `json:"Name"`
	ForezenQueuesNum       int    `json:"ForezenQueuesNum"`
	RepairQueueNamesNum    int    `json:"RepairQueueNamesNum"`
	LeftOverRepairQueueNum int    `json:"LeftOverRepairQueueNum"`
	LeftOverDelQueueNum    int    `json:"LeftOverDelQueueNum"`
	RepairCount            int64  `json:"RepairCount"`
	RepairFinished         bool   `json:"RepairFinished"`
}

type WALTimeRollQueue struct {
	// 每次roll的时候要加锁
	sync.RWMutex
	// 当前活跃的队列
	activeQueue Interface
	// 当前冷冻的队列
	forezenQueues []string
	// 当前恢复的队列
	activeRepairQueue       Interface
	leftOverDelQueueNames   []string
	leftOverRpairQueueNames []string
	repairQueueNames        []string
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
	// 保留时间
	rotation time.Duration
	// 滚动的时间间隔，单位为sint
	rollTimeSpan time.Duration
	// 下次切换的时间点
	nextRollTime int64
	// 每次调用repair process 函数出错后的回退时间
	backoffDuration time.Duration
	repairCount     int64
	// 每读取多少条消息，休息一会儿，来搞定流控
	limiterBatch int
	// 控制读取频率，每次读取后主动睡眠时间，防止读取过快
	limiterDuration time.Duration

	finishFlag bool
	exitFlag   uint32

	logf AppLogFunc
	rpf  RepairProcessFunc

	msgChan chan []byte
}

// 排序从大到小
type StringSlice []string

func (s StringSlice) Len() int { return len(s) }
func (s StringSlice) Less(i, j int) bool {
	return s[i] < s[j]
}
func (s StringSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (w *WALTimeRollQueue) Stats() *WALTimeRollQueueStats {

	leftOverRepairQueueNames, _ := w.GetLeftOverRepairQueueNames()

	stats := &WALTimeRollQueueStats{
		Name:                   w.Name,
		RepairCount:            atomic.LoadInt64(&w.repairCount),
		RepairFinished:         w.finishFlag,
		ForezenQueuesNum:       len(w.getForezenQueuesTimeStamps()),
		RepairQueueNamesNum:    len(w.GetRepairQueueNames()),
		LeftOverRepairQueueNum: len(leftOverRepairQueueNames),
		LeftOverDelQueueNum:    len(w.GetLeftOverDelQueueNames()),
	}

	return stats
}

func (w *WALTimeRollQueue) GetRepairQueueNames() []string {
	return w.repairQueueNames
}

func (w *WALTimeRollQueue) GetLeftOverDelQueueNames() []string {
	return w.leftOverDelQueueNames
}

func (w *WALTimeRollQueue) GetLeftOverRepairQueueNames() ([]string, error) {

	if w.finishFlag {
		return nil, nil
	}

	allRepairQueues := w.repairQueueNames

	activeRepairQueue := w.activeRepairQueue
	activeRepairQueueName := ""

	if activeRepairQueue == nil {
		return nil, nil
	} else {
		activeRepairQueueName = activeRepairQueue.GetName()
	}

	leftOverRepairQueue := []string{}
	for _, queue := range allRepairQueues {
		if queue >= activeRepairQueueName {
			leftOverRepairQueue = append(leftOverRepairQueue, queue)
		}
	}

	return leftOverRepairQueue, nil
}

func (w *WALTimeRollQueue) getAllQueueNames() ([]string, error) {
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

func (w *WALTimeRollQueue) getAllExpiredQueueTimeStamps() []int64 {

	allQueueName, err := w.getAllQueueNames()
	if err != nil {
		w.logf(ERROR, "WALTimeRollQueue getAllExpiredQueueTimeStamps strconv.Atoi %s", err)
		return nil
	}

	times := []int64{}

	for _, name := range allQueueName {
		t := strings.TrimPrefix(name, w.Name+"_")
		timestamp, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue getAllExpiredQueueTimeStamps strconv.Atoi %s", err)
		} else {
			if timestamp < time.Now().UnixNano()-w.rotation.Nanoseconds()-w.rollTimeSpan.Nanoseconds() {
				times = append(times, timestamp)
			}
		}
	}

	return times

}

func (w *WALTimeRollQueue) getNextRepairQueueName(name string) string {
	for _, repairName := range w.repairQueueNames {
		if name < repairName {
			return repairName
		}
	}
	return ""
}

func (w *WALTimeRollQueue) getForezenQueuesTimeStamps() []int64 {
	times := []int64{}
	forezenQueues := []string{}
	w.RLock()
	forezenQueues = append(forezenQueues, w.forezenQueues...)
	w.RUnlock()
	for _, name := range forezenQueues {
		t := strings.TrimPrefix(name, w.Name+"_")
		time, err := strconv.ParseInt(t, 10, 64)
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
		defer q.Close()
		return q.ResetReadMetaData()
	}
	return nil
}

func (w *WALTimeRollQueue) delteQueue(name string) error {
	q := New(name, w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	if q != nil {
		err := q.Empty()
		q.Close()
		if err != nil {
			return err
		} else {
			return os.Remove(path.Join(w.dataPath, name+".diskqueue.meta.dat"))
		}
	} else {
		os.Remove(path.Join(w.dataPath, name+".diskqueue.meta.dat"))
		return errors.New(name + " diskqueue is noneexistent")
	}
}

func (w *WALTimeRollQueue) GetNowActiveQueueName() string {
	return w.Name + "_" + strconv.FormatInt(time.Now().UnixNano()/int64(w.rollTimeSpan)*int64(w.rollTimeSpan), 10)
}

func (w *WALTimeRollQueue) GetNowActiveRepairQueueName() string {
	w.RLock()
	defer w.RUnlock()
	activeRepairQueue := w.activeRepairQueue
	if activeRepairQueue == nil {
		return ""
	} else {
		return activeRepairQueue.GetName()
	}
}

func (w *WALTimeRollQueue) GetNextRollTime() int64 {
	return time.Now().UnixNano()/int64(w.rollTimeSpan)*int64(w.rollTimeSpan) + int64(w.rollTimeSpan)
}

// 并发安全的，多个进程同时发起Roll，那么只有一个会成功
func (w *WALTimeRollQueue) Roll() {

	w.Lock()
	defer w.Unlock()

	if w.exitFlag > 0 {
		return
	}

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

	if w.exitFlag > 0 {
		return errors.New("WALTimeRollQueue exit Flag is true")
	}

	if time.Now().UnixNano() > w.nextRollTime {
		// 随机睡眠一下，防止大家共同进入加锁
		time.Sleep(time.Microsecond * time.Duration(rand.Intn(500)))
		w.Roll()
	}
	return w.activeQueue.Put(msg)

}

func (w *WALTimeRollQueue) SetRepairProcessFunc(rpf func(msg []byte) bool) {
	w.rpf = rpf
}

func (w *WALTimeRollQueue) repair() {

	// 开启一个读go程去从repair队列中将读出来，等待上层取走，一旦读通道空出来则开始读，第一次读的时候去返回一个空的消息
	for {
		// 如果发现退出标志为真，那么直接退出
		if w.exitFlag > 0 {
			return
		}

		msg, ok := w.ReadMsg()

		if !ok {
			w.finishFlag = true
			// 退出修复go程
			break
		}

		if msg == nil {
			continue
		}

		atomic.AddInt64(&w.repairCount, 1)

		for {
			ok := w.rpf(msg)
			if !ok {
				w.logf(ERROR, "WALTimeRollQueue process repair message failed")
				time.Sleep(w.backoffDuration)
			} else {
				break
			}
		}

		// 按照这个速度，恢复的时候最快也就是5w个点每秒
		if atomic.LoadInt64(&w.repairCount)%int64(w.limiterBatch) == 0 {
			time.Sleep(w.limiterDuration)
		}

	}

}

func (w *WALTimeRollQueue) Start() error {
	err := w.init()
	if err != nil {
		return err
	}

	if w.rpf != nil {
		go w.repair()
	}

	// 增加过期删除
	go func() {

		for {
			time.Sleep(5 * time.Minute)
			w.deleteExpired()
		}

	}()

	return nil
}

func (w *WALTimeRollQueue) ReadMsg() ([]byte, bool) {

	w.Lock()
	defer w.Unlock()

	if w.exitFlag > 0 {
		return nil, false
	}

	for {

		if w.activeRepairQueue == nil {
			return nil, false
		} else {

			if msg, ok := w.activeRepairQueue.ReadNoBlock(); ok {
				return msg, true
			} else {
				err := w.activeRepairQueue.Close()
				if err != nil {
					w.logf(ERROR, "WALTimeRollQueue Close activeRepairQueue err %s", err)
				}
				newRepairQueue := w.getNextRepairQueueName(w.activeRepairQueue.GetName())
				if newRepairQueue == "" {
					return nil, false
				}
				w.activeRepairQueue = New(newRepairQueue, w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
				continue
			}
		}

	}

}

func (w *WALTimeRollQueue) Close() {
	w.Lock()
	defer w.Unlock()

	if !atomic.CompareAndSwapUint32(&w.exitFlag, 0, 1) {
		return
	}

	if w.activeRepairQueue != nil {
		err := w.activeRepairQueue.Close()
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue Close activeRepairQueue err %s", err)
		}
	}

	if w.activeQueue != nil {
		err := w.activeQueue.Close()
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue activeQueue Close %s", err)
		}
	}

}

func (w *WALTimeRollQueue) DeleteForezenBefore(t int64) {
	forezenTimes := w.getForezenQueuesTimeStamps()
	restForezenQueue := []string{}
	for _, time := range forezenTimes {
		if time <= t*1000000000 {
			err := w.delteQueue(w.Name + "_" + strconv.FormatInt(time, 10))
			if err != nil {
				w.logf(ERROR, "WALTimeRollQueue DeleteForezenBefore delteQueue %s", err)
			}
		} else {
			restForezenQueue = append(restForezenQueue, w.Name+"_"+strconv.FormatInt(time, 10))
		}
	}

	w.Lock()
	w.forezenQueues = restForezenQueue
	w.Unlock()

}

func (w *WALTimeRollQueue) ResetRepairs() {
	for _, name := range w.GetRepairQueueNames() {
		err := w.resetQueue(name)
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue ResetRepairs resetQueue %s", err)
		}
	}
}

func (w *WALTimeRollQueue) DeleteRepairs() {
	activeRepairName := w.GetNowActiveRepairQueueName()
	leftOverNames := w.GetLeftOverDelQueueNames()
	if len(leftOverNames) == 0 {
		return
	}
	leftOver := []string{}
	for _, name := range leftOverNames {
		if activeRepairName == "" || name < activeRepairName {
			err := w.delteQueue(name)
			if err != nil {
				w.logf(ERROR, "WALTimeRollQueue DeleteRepairs delteQueue %s", err)
			}
		} else {
			leftOver = append(leftOver, name)
		}
	}
	w.Lock()
	w.leftOverDelQueueNames = leftOver
	w.Unlock()
}

func (w *WALTimeRollQueue) deleteExpired() {

	ts := w.getAllExpiredQueueTimeStamps()
	for _, time := range ts {
		err := w.delteQueue(w.Name + "_" + strconv.FormatInt(time, 10))
		if err != nil {
			w.logf(ERROR, "WALTimeRollQueue deleteExpired delteQueue %s", err)
		}
	}

}

func (w *WALTimeRollQueue) FinishRepaired() bool {
	return w.finishFlag
}

func NewTimeRollQueue(log AppLogFunc, options *Options) WALTimeRollQueueI {

	return &WALTimeRollQueue{
		Name:            options.Name,
		dataPath:        options.DataPath,
		maxBytesPerFile: options.MaxBytesPerFile,
		minMsgSize:      options.MinMsgSize,
		maxMsgSize:      options.MaxMsgSize,
		syncEvery:       options.SyncEvery,
		syncTimeout:     options.SyncTimeout,
		rollTimeSpan:    time.Duration(options.RollTimeSpanSecond) * time.Second,
		rotation:        time.Duration(options.RotationTimeSecond) * time.Second,
		backoffDuration: options.BackoffDuration,
		limiterBatch:    options.LimiterBatch,
		limiterDuration: options.LimiterDuration,
		logf:            log,
		msgChan:         make(chan []byte),
	}

}

func (w *WALTimeRollQueue) init() error {
	// 每次启动的时候用当前最新时间
	w.activeQueue = New(w.Name+"_"+strconv.FormatInt(time.Now().UnixNano(), 10), w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	w.nextRollTime = w.GetNextRollTime()
	w.forezenQueues = []string{}
	var err error
	// 删除过期的队列
	w.deleteExpired()
	w.repairQueueNames, err = w.getAllQueueNames()
	w.leftOverDelQueueNames = make([]string, len(w.repairQueueNames), len(w.repairQueueNames))
	copy(w.leftOverDelQueueNames, w.repairQueueNames)
	//刷新所有repair队列的元信息中的readPos， readNum
	w.ResetRepairs()
	if len(w.repairQueueNames) > 0 {
		w.activeRepairQueue = New(w.repairQueueNames[0], w.dataPath, w.maxBytesPerFile, w.minMsgSize, w.maxMsgSize, w.syncEvery, w.syncTimeout, w.logf)
	}
	if err != nil {
		return err
	}
	return nil

}
