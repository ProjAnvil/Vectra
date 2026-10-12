package vectra

import "sync"

// 位图缓冲池：BFS 的 visited 位图按字数复用，查询结束归还前清零。
type bitmapPool struct {
	p sync.Pool
}

func (bp *bitmapPool) acquire(words int) []uint32 {
	if v, ok := bp.p.Get().(*[]uint32); ok && len(*v) >= words {
		b := (*v)[:words]
		clear(b)
		return b
	}
	return make([]uint32, words)
}

func (bp *bitmapPool) release(b []uint32) {
	clear(b)
	bp.p.Put(&b)
}

var visitedPool bitmapPool

func wordsFor(n int) int { return (n + 31) / 32 }
