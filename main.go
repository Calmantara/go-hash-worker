package main

import (
	"fmt"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

type myWorker string

func (m myWorker) String() string {
	return string(m)
}

const (
	workerPool = 3
)

func worker(workerName myWorker, input <-chan string) {
	for {
		val := <-input
		fmt.Printf("worker name:%v value:%v\n", workerName, val)
	}
}

func main() {
	workerMap := make(map[myWorker]chan string)
	// init worker pool key channel
	for i := 0; i < workerPool; i++ {
		chanWorker := make(chan string)
		workerMap[myWorker(fmt.Sprintf("worker-%d", i))] = chanWorker
	}
	// init worker pool go routine
	for k, v := range workerMap {
		fmt.Println("generate worker pool: " + k.String())
		go worker(k, v)
	}
	// init consistent hash
	cfg := consistent.Config{
		PartitionCount:    7,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)
	// register worker map member
	for k := range workerMap {
		c.Add(k)
	}

	// testing
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%v", i)
		member := c.LocateKey([]byte(key))
		// sending channel
		workerMap[myWorker(member.String())] <- key
		time.Sleep(time.Second)
	}
	// close channel
	for _, ch := range workerMap {
		close(ch)
	}
}
