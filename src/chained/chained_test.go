package chained

import (
	"hash/fnv"
	"strconv"
	"sync"
	"testing"
	"unsafe"
)

type mockValueFn func() uint64

type mockHash64 struct {
	callback mockValueFn
}

func (m *mockHash64) Sum64() uint64 {
	return m.callback()
}
func (m *mockHash64) Sum(b []byte) []byte {
	return nil
}

func (m *mockHash64) Reset() {
}

func (m *mockHash64) Size() int {
	return 0
}

func (m *mockHash64) BlockSize() int {
	return 0
}

func (m *mockHash64) Write(p []byte) (n int, err error) {
	return 0, nil
}

func newMockHash(c mockValueFn) *mockHash64 {
	mock := new(mockHash64)
	mock.callback = c
	return mock
}

func TestSizeNode(t *testing.T) {

	c := NewChainedHash(1, fnv.New64a())

	size := unsafe.Sizeof(c.arr[0])
	t.Log(size)

}

func TestSizeExt(t *testing.T) {

	var e ext
	size := unsafe.Sizeof(e)
	t.Log(size)

}

func TestAdd_IndexMap(t *testing.T) {

	fn := func() uint64 {
		return uint64(1)
	}
	c := newMockHash(fn)
	h := NewChainedHash(2, c)

	h.coder = newMockHash(func() uint64 { return 1 })
	h.Put("a", "a")

	h.coder = newMockHash(func() uint64 { return 2 })
	h.Put("b", "b")

	if h.arr[1].entry == nil || h.arr[1].entry.value != "a" {
		t.Fatal("value does not match for key: a")
	}

	if h.arr[2].entry == nil || h.arr[2].entry.value != "b" {
		t.Fatal("value does not match for key: b")
	}

}

func TestAdd_NodeNextSet(t *testing.T) {

	fn := func() uint64 {
		return uint64(1)
	}
	c := newMockHash(fn)
	h := NewChainedHash(2, c)

	h.coder = newMockHash(func() uint64 { return 1 })
	h.Put("a", "a")

	h.coder = newMockHash(func() uint64 { return 1 })
	h.Put("b", "b")

	if h.arr[1].next == nil {
		t.Fatal("value does not match for key: next is nil")
	} else if h.arr[1].next.entry[0] == nil {
		t.Fatal("value does not match for key: next.entry is nil")
	} else if h.arr[1].next.entry[0].value != "b" {
		t.Fatal("value does not match for key: b")
	}

}

func TestAdd_ExtIndexSet(t *testing.T) {

	fn := func() uint64 {
		return uint64(1)
	}
	c := newMockHash(fn)
	h := NewChainedHash(2, c)

	h.coder = newMockHash(func() uint64 { return 1 })
	h.Put("a", "a")
	h.Put("b", "b") //0
	h.Put("c", "c") //1
	h.Put("d", "d") //2

	if h.arr[1].next == nil {
		t.Fatal("value does not match for key: next is nil")
	} else if h.arr[1].next.entry[2] == nil {
		t.Fatal("value does not match for key: next.entry is nil")
	} else if h.arr[1].next.entry[2].value != "d" {
		t.Fatal("value does not match for key: d")
	}

}

func TestAdd_NextSetParallel(t *testing.T) {

	fn := func() uint64 {
		return uint64(1)
	}
	c := newMockHash(fn)
	h := NewChainedHash(2, c)

	done := make(chan bool)
	running := make(chan bool)
	n := 10

	var m sync.Mutex
	cond := sync.NewCond(&m)
	allset := false

	for i := 0; i < n; i++ {
		go func() {

			cond.L.Lock()
			running <- true
			for !allset {
				cond.Wait()
			}
			cond.L.Unlock()

			h.coder = newMockHash(fn)
			h.Put(strconv.Itoa(i), strconv.Itoa(i))
			done <- true

		}()
	}

	// wait for everything to start running
	for i := 0; i < n; i++ {
		<-running
		if i == n-1 {
			cond.L.Lock()
			allset = true
			cond.L.Unlock()
		}
	}

	// now signal all go routines to proceed with their work
	cond.L.Lock()
	cond.Broadcast()
	cond.L.Unlock()

	// wait for everyting to finish
	for i := 0; i < n; i++ {
		<-done
	}

	counter := 1
	for p := h.arr[1].next; p != nil; p = p.next {
		counter += 3
	}
	if counter != n {
		t.Fail()
	}
}
