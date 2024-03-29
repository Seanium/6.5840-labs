package raft

type Entry struct {
	Term    int
	Command interface{}
}

type Log struct {
	Log    []Entry // 下标为0的位置总是存储快照中最后一条entry, 从1开始才是当前log中包含的entry
	Index0 int     // 表示切片下标为0的位置所储存的entry index, 用于在下面的方法中转换切片下标与entry index
}

func mkLogEmpty() Log {
	return Log{make([]Entry, 1), 0} // 切片0位置存储1号log
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(e Entry) {
	l.Log = append(l.Log, e)
}

func (l *Log) start() int {
	return l.Index0
}

// 去掉>=index的entry
func (l *Log) cutend(index int) {
	l.Log = l.Log[0 : index-l.Index0]
}

// 去掉<index的entry
func (l *Log) cutstart(index int) {
	l.Log = l.Log[index-l.Index0:]
	l.Index0 += index - l.Index0
}

func (l *Log) slice(index int) []Entry {
	return l.Log[index-l.Index0:]
}

func (l *Log) lastindex() int {
	return l.Index0 + len(l.Log) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.Log[index-l.Index0])
}

func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}
