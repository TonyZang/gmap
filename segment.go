package gmap

import (
	"fmt"
	"gmap/base"
	"gmap/util"
	"sync"
	"sync/atomic"
)

type Segment interface {
	Put(p Pair) (bool, error)

	Get(key string) Pair

	GetWithHash(key string, keyHash uint64) Pair

	Delete(key string) bool

	Size() uint64
}

type segment struct {
	// 散列桶切片
	buckets []Bucket

	// 散列桶切片长度
	bucketsLen int

	// 键值对总数
	pairTotal uint64

	// 键值对的再分布器
	pairRedistributor PairRedistributor

	lock sync.Mutex
}

func newSegment(
	bucketNumber int, pairRedistributor PairRedistributor) Segment {
	if bucketNumber <= 0 {
		bucketNumber = base.DEFAULT_BUCKET_NUMBER
	}
	if pairRedistributor == nil {
		pairRedistributor = newDefaultPairRedistributor(base.DEFAULT_BUCKET_LOAD_FACTOR, bucketNumber)
	}
	buckets := make([]Bucket, bucketNumber)
	for i := 0; i < bucketNumber; i++ {
		buckets[i] = newBucket()
	}
	return &segment{
		buckets:           buckets,
		bucketsLen:        bucketNumber,
		pairRedistributor: pairRedistributor,
	}
}

func (s *segment) Put(p Pair) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	b := s.buckets[int(p.Hash()%uint64(s.bucketsLen))]
	ok, err := b.Put(p, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, 1)
		s.redistribe(newTotal, b.Size())
	}
	return ok, err
}

func (s *segment) Get(key string) Pair {
	return s.GetWithHash(key, util.Hash(key))
}

func (s *segment) GetWithHash(key string, keyHash uint64) Pair {
	s.lock.Lock()
	defer s.lock.Unlock()
	b := s.buckets[int(keyHash%uint64(s.bucketsLen))]
	return b.Get(key)
}

func (s *segment) Delete(key string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	b := s.buckets[int(util.Hash(key)%uint64(s.bucketsLen))]
	ok := b.Delete(key, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, ^uint64(0))
		s.redistribe(newTotal, b.Size())
	}
	return ok
}

func (s *segment) Size() uint64 {
	return atomic.LoadUint64(&s.pairTotal)
}

func (s *segment) redistribe(pairTotal uint64, bucketSize uint64) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if pErr, ok := p.(error); ok {
				err = newPairRedistributorError(pErr.Error())
			} else {
				err = newPairRedistributorError(fmt.Sprintf("%s", p))
			}
		}
	}()

	s.pairRedistributor.UpdateThreshold(pairTotal, s.bucketsLen)
	bucketStatus := s.pairRedistributor.CheckBucketStatus(pairTotal, bucketSize)
	newBuckets, changed := s.pairRedistributor.Redistribe(bucketStatus, s.buckets)
	if changed {
		s.buckets = newBuckets
		s.bucketsLen = len(s.buckets)
	}
	return nil
}
