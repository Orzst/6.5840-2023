package raft

// 对log定义一个数据结构并封装一些常用操作
type raftLog struct {
	Index0  int
	Entries []Entry
}

// 表示RaftLog的entry
type Entry struct {
	Term    int
	Command interface{} // 表示与具体service相关的操作
}

func newRaftLog(index0 int) raftLog {
	return raftLog{Index0: index0, Entries: []Entry{}}
}

func (log *raftLog) at(index int) Entry {
	// 注意处理index传入为0的情况
	if index == 0 {
		return Entry{Term: -1}
	} else if index > log.Index0 {
		index -= log.Index0 + 1
		return log.Entries[index]
	}
	// index <= log.index0的情况还没处理
	// 这里随便return一个
	return Entry{Term: -2}
}

func (log *raftLog) lastIndex() int {
	return log.Index0 + len(log.Entries)
}

func (log *raftLog) firstIndexOfTerm(term int) int {
	// 后面引入快照之后这里大概率要改
	index := -1
	for i := log.lastIndex(); i >= 1; i-- {
		if log.at(i).Term == term {
			index = i
		}
	}
	return index
}

func (log *raftLog) lastIndexOfTerm(term int) int {
	// 后面引入快照之后这里大概率要改
	index := -1
	for i := log.lastIndex(); i >= 1; i-- {
		if log.at(i).Term == term {
			index = i
			break
		}
	}
	return index
}

func (log *raftLog) append(entry ...Entry) {
	log.Entries = append(log.Entries, entry...)
}

func (log *raftLog) slice(start, end int) []Entry {
	ret := []Entry{}
	// 注意一些情况下，比如刚开始没有entry时，可能会调用slice(0, x)，所以这里特别处理一下
	if start == 0 {
		ret = append(ret, Entry{Term: -1})
		start++
	}
	// start 和 end可能 < log.index0的情况还没处理
	if start > log.Index0 && end > start {
		start -= log.Index0 + 1
		end -= log.Index0 + 1
		ret = append(ret, log.Entries[start:end]...)
	}
	return ret
}

func (log *raftLog) truncate(index int) {
	index -= log.Index0 + 1
	log.Entries = log.Entries[:index]
}
