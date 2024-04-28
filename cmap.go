package gmap

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
