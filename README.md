# go-diskqueue

[![Build Status](https://secure.travis-ci.org/nsqio/go-diskqueue.png?branch=master)](http://travis-ci.org/nsqio/go-diskqueue) [![GoDoc](https://godoc.org/github.com/nsqio/go-diskqueue?status.svg)](https://godoc.org/github.com/nsqio/go-diskqueue) [![GitHub release](https://img.shields.io/github/release/nsqio/go-diskqueue.svg)](https://github.com/nsqio/go-diskqueue/releases/latest)

A Go package providing a filesystem-backed FIFO queue

Pulled out of https://github.com/nsqio/nsq

## 说明：基于go-diskqueue的改造版本
- 去除原先depth属性
- 去除自动轮转与自动删除的策略，为了适应某些场景需要重新读取队列
- 增加基于时间轮转的功能，每过一个时间周期，轮转一次队列的实际存储名字
- 此roll queue将写入记录持续不断地写入进来，并且保证数据至少被消费一遍
- 如果调用者不手动提交删除操作，则队列不会删除，在重启后，队列里面的数据会被再消费一遍，所以说重复读是大概率事情
- 此队列的场景是保证数据不丢，而不是保持消费一次

