package diskqueue

import "time"

const (
	DefaultName            = "timerollqueue"
	DefaultDataPath        = "."
	DefaultMaxBytesPerFile = 100 * 1024 * 1024
	DefaultMinMsgSize      = 32
	DefaultMaxMsgSize      = 4 * 1024 * 1024
	DefaultSyncEvery       = 5000
	DefaultSyncTimeout     = 2 * time.Second
)

type Options struct {
	Name            string        `json:"WALTimeRollQueue.Name"`
	DataPath        string        `json:"WALTimeRollQueue.DataPath"`
	MaxBytesPerFile int64         `json:"WALTimeRollQueue.MaxBytesPerFile"`
	MinMsgSize      int64         `json:"WALTimeRollQueue.MinMsgSize"`
	MaxMsgSize      int64         `json:"WALTimeRollQueue.MaxMsgSize"`
	SyncEvery       int64         `json:"WALTimeRollQueue.SyncEvery"`
	SyncTimeout     time.Duration `json:"WALTimeRollQueue.SyncTimeout"`
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
	}
}

type WALTimeRollQueueI interface {
	// 生产消息
	Put([]byte) error
	// 启动， 一旦启动会开启一个新的activeQueue，并且从磁盘中获取所有的队列，将其放入repair列表
	Start() error
	// 重启， 一旦重启那么所有在磁盘的上的队列会李曼变成repairQueues，会重新消费一遍
	Restart() error
	// 读取消息，只会从repair队列中读取消息，不会去读取当前队列的消息
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	// 关闭：关闭activeQueue并且同步磁盘
	Close() error
	// 删除冷冻队列
	DeleteForezen() error
	// 删除repair队列
	DeleteRepairs() error
}

type WALTimeRollQueue struct {
	// 当前活跃的队列
	activeQueue Interface
	// 当前冷冻的队列
	forezenQueue Interface
	// 当前恢复的队列
	repairQueues []Interface
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
}

func (w *WALTimeRollQueue) Put(msg []byte) error {
	return nil
}
func (w *WALTimeRollQueue) Start() error {
	return nil
}
func (w *WALTimeRollQueue) Restart() error {
	return nil
}
func (w *WALTimeRollQueue) ReadChan() <-chan []byte {
	return nil
}
func (w *WALTimeRollQueue) Close() error {
	return nil
}
func (w *WALTimeRollQueue) DeleteForezen() error {
	return nil
}
func (w *WALTimeRollQueue) DeleteRepairs() error {
	return nil
}

func NewTimeRollQueue(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) WALTimeRollQueueI {

	if name == "" {

	}

	return &WALTimeRollQueue{
		activeQueue: New(name),
	}

}
