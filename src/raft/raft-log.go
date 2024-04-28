package raft

import (
	"fmt"
	goLog "log"
)

// 对log定义一个数据结构并封装一些常用操作
type raftLog struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Entries           []Entry
}

// 表示RaftLog的entry
type Entry struct {
	Term    int
	Command interface{} // 表示与具体service相关的操作
}

func newRaftLog() raftLog {
	return raftLog{
		LastIncludedIndex: 0,
		LastIncludedTerm:  -1,
		Entries:           []Entry{},
	}
}

// 确保index >= log.LastIncludedIndex
func (log *raftLog) checkIndexValid(index int, infoPrefix string) {
	if index < log.LastIncludedIndex {
		panic(fmt.Sprintf("index = %d, %s, 无法得到快照中包含的除最后一个以外的entry的信息", index, infoPrefix))
	}
	if index > log.lastIndex() {
		panic(fmt.Sprintf("index = %d, %s, 指定的index大于实际有的最大index", index, infoPrefix))
	}
}

func (log *raftLog) at(index int) Entry {
	log.checkIndexValid(index, "raftLog.at(index)")
	if index > log.LastIncludedIndex {
		index -= log.LastIncludedIndex + 1
		return log.Entries[index]
	} else {
		//	条件即index == log.LastIncludedIndex
		return Entry{Term: log.LastIncludedTerm}
	}
}

func (log *raftLog) lastIndex() int {
	return log.LastIncludedIndex + len(log.Entries)
}

func (log *raftLog) firstIndexOfTerm(term int) int {
	// 注意可能当前term的部分log已经在快照中，所以未必是真正的first index
	index := -1
	for i := log.lastIndex(); i > log.LastIncludedIndex; i-- {
		if log.at(i).Term == term {
			index = i
		}
	}
	if index != -1 {
		return index
	} else {
		panic("所要寻找的term的log可能已因有快照而丢弃") // 应该是不会运行这一行的
	}
}

func (log *raftLog) lastIndexOfTerm(term int) int {
	index := -1
	for i := log.lastIndex(); i > log.LastIncludedIndex; i-- {
		if log.at(i).Term == term {
			index = i
			break
		}
	}
	if index != -1 {
		return index
	} else {
		panic("所要寻找的term的log可能已因有快照而丢弃") // 应该是不会运行这一行的
	}
}

func (log *raftLog) append(entry ...Entry) {
	log.Entries = append(log.Entries, entry...)
}

func (log *raftLog) slice(start, end int) []Entry {
	if end < start {
		goLog.Fatalf("raftLog.slice(start, end)中end < start")
	}
	ret := make([]Entry, 0, end-start)
	if end == start {
		return ret
	}
	// 注意一些情况下，可能会调用slice(log.LastIncludedIndex, x)，所以这里特别处理一下
	log.checkIndexValid(start, "raftLog.slice(start, end)")
	if start == log.LastIncludedIndex {
		panic(fmt.Sprintf("发送的Entries起点为lastIncludedIndex"))
	}
	// 到这里保证了end >= start > log.LastIncludedIndex
	start -= log.LastIncludedIndex + 1
	end -= log.LastIncludedIndex + 1
	ret = append(ret, log.Entries[start:end]...)
	return ret
}

func (log *raftLog) truncate(index int) {
	log.checkIndexValid(index, "raftLog.truncate(index)")
	index -= log.LastIncludedIndex + 1
	log.Entries = log.Entries[:index]
}

func (log *raftLog) trimBefore(newLastIncludedIndex int) {
	log.checkIndexValid(newLastIncludedIndex, "raftLog.trimBefore(newLastIncludedIndex)")
	log.LastIncludedTerm = log.at(newLastIncludedIndex).Term
	log.Entries = log.Entries[newLastIncludedIndex-log.LastIncludedIndex:]
	log.LastIncludedIndex = newLastIncludedIndex
}

func (log *raftLog) clearAndReset(newLastIncludedIndex, newLastIncludedTerm int) {
	log.LastIncludedIndex = newLastIncludedIndex
	log.LastIncludedTerm = newLastIncludedTerm
	log.Entries = []Entry{}
}
