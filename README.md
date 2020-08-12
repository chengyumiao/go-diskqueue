# go-diskqueue

[![Build Status](https://secure.travis-ci.org/nsqio/go-diskqueue.png?branch=master)](http://travis-ci.org/nsqio/go-diskqueue) [![GoDoc](https://godoc.org/github.com/nsqio/go-diskqueue?status.svg)](https://godoc.org/github.com/nsqio/go-diskqueue) [![GitHub release](https://img.shields.io/github/release/nsqio/go-diskqueue.svg)](https://github.com/nsqio/go-diskqueue/releases/latest)

A Go package providing a filesystem-backed FIFO queue

Pulled out of https://github.com/nsqio/nsq

## 说明：基于go-diskqueue的改造版本
- 去除原先depth属性
- 去除自动轮转与自动删除的策略，为了适应某些场景需要重新读取队列
- 增加基于时间轮转的功能，每过一个时间周期，轮转一次队列的实际存储名字
- 

