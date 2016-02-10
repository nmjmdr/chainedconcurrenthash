package chained

import (
	"testing"
	"hash/fnv"
	"math"
	"strconv"
	"sync"	
)

func BenchmarkAddFirstGetLaterLoadFactor50_1000Size(b *testing.B) {
	for i:=0; i<b.N;i++ {
		addFirstGetLater(10,0.50)
	}

}

func BenchmarkAddFirstGetLaterLoadFactor50_1000Size_BuiltIn(b *testing.B) {

	for i:=0; i<b.N;i++ {
		addFirstGetLaterBuiltIn(10,0.50)
	}

}

func BenchmarkAddFirstGetLaterLoadFactor50_LargeSize16(b *testing.B) {
	for i:=0; i<b.N;i++ {
		addFirstGetLater(16,0.50)
	}

}

func BenchmarkAddFirstGetLaterLoadFactor50_LargeSize16_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		addFirstGetLaterBuiltIn(16,0.50)
	}

}


func BenchmarkAddFirstGetLaterLoadFactor50_LargeSize20(b *testing.B) {
	for i:=0; i<b.N;i++ {
		addFirstGetLater(20,0.50)
	}

}

func BenchmarkAddFirstGetLaterLoadFactor50_LargeSize20_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		addFirstGetLaterBuiltIn(20,0.50)
	}

}



func BenchmarkParallelPutAndGet_90Load_16Size(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetParallel(16,0.90)
	}
}


func putGetParallel(power int,factor float64) {
	var h Hashmap
	h = NewChainedHash(power,fnv.New64a())

	n := uint64(math.Pow(2,float64(power)))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
	
		for i:=uint64(0);i<uint64((float64(n)*factor));i++ {
			h.Put(strconv.FormatUint(i,10),strconv.FormatUint(i,10))
		}		
		wg.Done()
	}()
	
	
	go func(){
		
		for cycle:=0;cycle<5;cycle++ {
			for i:=uint64(0);i<n;i++ {
				h.Get(strconv.FormatUint(i,10))
			}
		}		
		wg.Done()
	}()


	wg.Wait()

}


func BenchmarkParallelPutAndGet_90Load_16Size_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetParallel(16,0.90)
	}
}


func putGetParallel_BuiltIn(power int,factor float64) {
	


	n := uint64(math.Pow(2,float64(power)))

	h := make(map[string]string,n)

	var wg sync.WaitGroup
	wg.Add(2)

	var mutex sync.Mutex

	go func() {
	
		for i:=uint64(0);i<uint64((float64(n)*factor));i++ {
			mutex.Lock()
			h[strconv.FormatUint(i,10)] = strconv.FormatUint(i,10)
			mutex.Unlock()
		}		
		wg.Done()
	}()
	
	
	go func(){
		
		for cycle:=0;cycle<5;cycle++ {
			for i:=uint64(0);i<n;i++ {
				mutex.Lock()
				_,_ = h[strconv.FormatUint(i,10)]
				mutex.Unlock()
			}
		}		
		wg.Done()
	}()


	wg.Wait()

}


// -- Higher concurrency tests
func BenchmarkParallelPutAndGet_90Load_16Size_4Concurrent(b *testing.B) {

	for i:=0; i<b.N;i++ {
		putGetConcurrent(16,0.90,4)
	}
}


func BenchmarkParallelPutAndGet_90Load_16Size_4Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(16,0.90,4)
	}
}


func BenchmarkParallelPutAndGet_90Load_16Size_10Concurrent(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent(16,0.90,10)
	}
}


func BenchmarkParallelPutAndGet_90Load_16Size_10Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(16,0.90,10)
	}
}


func BenchmarkParallelPutAndGet_90Load_16Size_100Concurrent(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent(16,0.90,100)
	}
}


func BenchmarkParallelPutAndGet_90Load_16Size_100Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(16,0.90,100)
	}
}


func BenchmarkParallelPutAndGet_10Load_16Size_100Concurrent(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent(16,0.10,100)
	}
}


func BenchmarkParallelPutAndGet_10Load_16Size_100Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(16,0.10,100)
	}
}


func BenchmarkParallelPutAndGet_98Load_16Size_100Concurrent(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent(16,0.98,100)
	}
}


func BenchmarkParallelPutAndGet_98Load_16Size_100Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(16,0.98,100)
	}
}

func BenchmarkParallelPutAndGet_50Load_16Size_50Concurrent(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent(16,0.50,50)
	}
}


func BenchmarkParallelPutAndGet_50Load_16Size_50Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(16,0.50,50)
	}
}

func BenchmarkParallelPutAndGet_50Load_20Size_50Concurrent(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent(20,0.50,50)
	}
}


func BenchmarkParallelPutAndGet_50Load_20Size_50Concurrent_BuiltIn(b *testing.B) {
	for i:=0; i<b.N;i++ {
		putGetConcurrent_BuiltIn(20,0.50,50)
	}
}

func putGetConcurrent(power int,factor float64,concurrency int) {
	var h Hashmap
	h = NewChainedHash(power,fnv.New64a())

	n := math.Pow(2,float64(power))

	var wg sync.WaitGroup
	wg.Add(concurrency)
	wg.Add(concurrency)

	
	start := uint64(0)
	step := (uint64(n*factor)/uint64(concurrency) - 1)
	end := start + step
	

	for i:=0;i<concurrency;i++ {

		go func(start uint64,end uint64) {
	
		for i:=start;i<end;i++ {
			h.Put(strconv.FormatUint(i,10),strconv.FormatUint(i,10))
		}		
		wg.Done()
		}(start,end)

		start = end
		end = end + step
	}
	
		
	
	for i:=0;i<concurrency;i++ {
		go func(){
			
			for cycle:=0;cycle<5;cycle++ {
				for i:=uint64(0);i<uint64(n);i++ {
					h.Get(strconv.FormatUint(i,10))
				}
			}		
			wg.Done()
		}()
	}

	wg.Wait()

}





func putGetConcurrent_BuiltIn(power int,factor float64,concurrency int) {
	


	n := math.Pow(2,float64(power))
	m := make(map[string]string,uint64(n))

	var wg sync.WaitGroup
	wg.Add(concurrency)
	wg.Add(concurrency)


	var rwmutex sync.RWMutex

	start := uint64(0)
	step := (uint64(n*factor)/uint64(concurrency) - 1)
	end := start + step
	

	for i:=0;i<concurrency;i++ {

		go func(start uint64,end uint64) {
	
		for i:=start;i<end;i++ {
			rwmutex.Lock()
			m[strconv.FormatUint(i,10)] = strconv.FormatUint(i,10)
			rwmutex.Unlock()
		}		
		wg.Done()
		}(start,end)

		start = end
		end = end + step
	}
	
		
	
	for i:=0;i<concurrency;i++ {
		go func(){
			
			for cycle:=0;cycle<5;cycle++ {
				for i:=uint64(0);i<uint64(n);i++ {
					rwmutex.RLock()
					_,_ = m[strconv.FormatUint(i,10)]
					rwmutex.RUnlock()
				}
			}		
			wg.Done()
		}()
	}

	wg.Wait()

}





func addFirstGetLater(power int,factor float64) {
	var h Hashmap
	h = NewChainedHash(power,fnv.New64a())

	n := math.Pow(2,float64(power))


	for i:=uint64(0);i<uint64(n*factor);i++ {
		h.Put(strconv.FormatUint(i,10),strconv.FormatUint(i,10))
	}

	
	for i:=uint64(0);i<uint64(n);i++ {
		h.Get(strconv.FormatUint(i,10))
	}

}



func addFirstGetLaterBuiltIn(power float64,factor float64) {
		
	n := math.Pow(2,float64(power))

	m := make(map[string]string,uint64(n))


	for i:=uint64(0);i<uint64(n*factor);i++ {
		m[strconv.FormatUint(i,10)] = strconv.FormatUint(i,10)
	}


	for i:=uint64(0);i<uint64(n);i++ {
		_,_ = m[strconv.FormatUint(i,10)]
	}

}
