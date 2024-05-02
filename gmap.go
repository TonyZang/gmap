package gmap

import (
	"gmap/base"
	"gmap/util"
	"math"
	"sync/atomic"
)

type ConcurrentMap interface {
	Concurrency() int
	Put(key string, element interface{}) (bool, error)
	Get(key string) interface{}
	Delete(key string) bool
	Len() uint64
}

type myConcurrentMap struct {
	concurrency int
	segments    []Segment
	total       uint64
}

func NewConcurrentMap(
	concurrency int,
	pairRedistributor PairRedistributor) (ConcurrentMap, error) {
	if concurrency <= 0 {
		return nil, newIllegalParameterError("concurrency is too small")
	}
	if concurrency > base.MAX_CONCURRENCY {
		return nil, newIllegalParameterError("concurrency is too large")
	}
	cmap := &myConcurrentMap{}
	cmap.concurrency = concurrency
	cmap.segments = make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		cmap.segments[i] =
			newSegment(base.DEFAULT_BUCKET_NUMBER, pairRedistributor)
	}
	return cmap, nil
}

func (mc *myConcurrentMap) Concurrency() int {
	return mc.concurrency
}

func (mc *myConcurrentMap) Put(key string, element any) (bool, error) {
	p, err := newPair(key, element)
	if err != nil {
		return false, err
	}
	s := mc.findSegment(p.Hash())
	ok, err := s.Put(p)
	if ok {
		atomic.AddUint64(&mc.total, 1)
	}
	return ok, err
}

// findSegment 会根据给定参数寻找并返回对应散列段。
func (cmap *myConcurrentMap) findSegment(keyHash uint64) Segment {
	if cmap.concurrency == 1 {
		return cmap.segments[0]
	}
	var keyHashHigh int
	if keyHash > math.MaxUint32 {
		keyHashHigh = int(keyHash >> 48)
	} else {
		keyHashHigh = int(keyHash >> 16)
	}
	return cmap.segments[keyHashHigh%cmap.concurrency]
}

func (mc *myConcurrentMap) Get(key string) any {
	keyHash := util.Hash(key)
	s := mc.findSegment(keyHash)
	pair := s.GetWithHash(key, keyHash)
	if pair == nil {
		return nil
	}
	return pair.Element()
}

func (mc *myConcurrentMap) Delete(key string) bool {
	s := mc.findSegment(util.Hash(key))
	if s.Delete(key) {
		atomic.AddUint64(&mc.total, ^uint64(0))
		return true
	}
	return false
}

func (mc *myConcurrentMap) Len() uint64 {
	return atomic.LoadUint64(&mc.total)
}
