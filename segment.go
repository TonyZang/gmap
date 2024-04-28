package gmap

type Segment interface {
	Put(p Pair) (bool, error)

	Get(key string) Pair

	GetWithHash(key string, keyHash uint64) Pair

	Delete(key string) bool

	Size() uint64
}

type segment struct {
	buckets []Bucket

	bucketsLen int

	pairTotal uint64

	pairRedistributor PairRedistributor
}
