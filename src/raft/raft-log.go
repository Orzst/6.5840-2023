package raft

// 对log定义一个数据结构并封装一些常用操作
type raftLog struct {
	index0  int
	entries []Entry
}

// 表示RaftLog的entry
type Entry struct {
	Term    int
	Command interface{} // 表示与具体service相关的操作
}

func newRaftLog(index0 int) raftLog {
	return raftLog{index0: index0, entries: []Entry{}}
}

func (log *raftLog) at(index int) Entry {
	// 注意处理index传入为0的情况
	if index == 0 {
		return Entry{Term: -1}
	} else if index > log.index0 {
		index -= log.index0 + 1
		return log.entries[index]
	}
	// index <= log.index0的情况还没处理
	// 这里随便return一个
	return Entry{Term: -2}
}

func (log *raftLog) lastIndex() int {
	return log.index0 + len(log.entries)
}

func (log *raftLog) append(entry ...Entry) {
	log.entries = append(log.entries, entry...)
}

func (log *raftLog) slice(start, end int) []Entry {
	ret := []Entry{}
	// 注意一些情况下，比如刚开始没有entry时，可能会调用slice(0, x)，所以这里特别处理一下
	if start == 0 {
		ret = append(ret, Entry{Term: -1})
		start++
	}
	// start 和 end可能 < log.index0的情况还没处理
	if start > log.index0 && end > start {
		start -= log.index0 + 1
		end -= log.index0 + 1
		ret = append(ret, log.entries[start:end]...)
	}
	return ret
}

func (log *raftLog) truncate(index int) {
	index -= log.index0 + 1
	log.entries = log.entries[:index]
}
