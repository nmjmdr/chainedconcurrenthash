package chained

import (
	"math"
	"hash"
	"sync/atomic"
	"errors"	
)

type entry struct {
	key string
	value string
}

type node struct {
	hashkey uint64
	entry *entry
	nextLock int32
	next *ext
}


type ext struct {
	hashkey [3]uint64
	entry [3](*entry)
	nextLock int32
	next *ext
}

type Hashmap interface {
	Put(key string,value string) (error)
	Get(key string) (string,bool)
	Update(key string,value string) error 
}



type chainedHash struct {
	arr []node
	coder hash.Hash64
}

func NewChainedHash(power int,coder hash.Hash64) *chainedHash {
	c := new(chainedHash)
	c.coder = coder
	initSize := uint64(math.Pow(2,float64(power)))
	c.arr = make([]node,initSize)

	return c
}

func (h *chainedHash) Update(key string,value string) error {
	
	h.coder.Write([]byte(key))
	hashkey := h.coder.Sum64()

	index := hashkey % uint64(len(h.arr))

	if h.arr[index].hashkey == hashkey && h.arr[index].entry != nil && h.arr[index].entry.key == key  {
		h.arr[index].entry.value = value
		return nil
	}

	for p:=h.arr[index].next; p!=nil; p=p.next {
		for k:=0; k<3; k++ {
			if p.hashkey[k] == hashkey && p.entry[k] != nil && p.entry[k].key == key {
				p.entry[k].value = value
				return nil
			} 
		}
	}

	return errors.New("Key not found")
}

func (h *chainedHash) Put(key string,value string) error {
	
	h.coder.Write([]byte(key))
	hashkey := h.coder.Sum64()

	index := hashkey % uint64(len(h.arr))

	e := new(entry)
	e.key = key
	e.value = value

	// check if arr[index].hashkey  is 0, if yes then set to new hashkey and the entry
	// this assumes that the hash function supplied does not produce a zero hashvalue
	if atomic.CompareAndSwapUint64(&(h.arr[index].hashkey),0,hashkey) {
		h.arr[index].entry = e
		return nil
	} 

	

	

	


	// we could not allocate at the index, we should try and find an empty spot
	// to insert
	for {



		// check if we the indexe's next node is empty, if yes then allocate an ext
		// and proceed return
		if h.arr[index].next == nil {
			ex := new(ext)
			if atomic.CompareAndSwapInt32(&(h.arr[index].nextLock),0,1) {
				// then allocate
				// these have to be atomic operations
				// or else other might end up using the slots
				if atomic.CompareAndSwapUint64(&(ex.hashkey[0]),0,hashkey) {
					ex.entry[0] = e
					// ideally this should be atomic store
					h.arr[index].next = ex
					return nil
				}
				// else some other thread beat us, 
				//try and find next available spot 	
			}
			// allow ex to be garbage collected
			ex = nil
		}


		var last *ext
		last = nil
			
		for p := h.arr[index].next; p!=nil; p = p.next {
			for k:=0;k<3;k++ {
				if atomic.CompareAndSwapUint64(&(p.hashkey[k]),0,hashkey) {
					p.entry[k] = e
					return nil
				}
			}
			last = p
		}

		if last == nil {
			// we need to continue, 
			//we assumed that last would have been set by now
			// but the oter thread that set h.arr[index].nextLock 
			// has not yet the next pointer
			continue
		}

		// we could not find a slot on existing nodes
		// try and allocate a new one
		ex := new(ext)
		ex.hashkey[0] = hashkey
		ex.entry[0] = e
		if atomic.CompareAndSwapInt32(&(last.nextLock),0,1) {
			// ideally this should be atomic store
			last.next = ex
			return nil
		} else {
			// some other thread beat us
			ex.entry[0] = nil
			ex = nil
		}
	}
	return nil

}


func (h *chainedHash) Get(key string) (string,bool) {
	
	h.coder.Write([]byte(key))
	hashkey := h.coder.Sum64()

	index := hashkey % uint64(len(h.arr))

	if h.arr[index].hashkey == hashkey && h.arr[index].entry != nil	&& h.arr[index].entry.key == key  {
		return h.arr[index].entry.value,true
	}

	for p:=h.arr[index].next; p!=nil; p=p.next {
		for k:=0; k<3; k++ {
			if p.hashkey[k] == hashkey && p.entry[k] != nil	&& p.entry[k].key == key {
				return p.entry[k].value,true
			} 
		}
	}

	return "",false

}





