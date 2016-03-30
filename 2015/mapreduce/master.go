package mapreduce

import "container/list"
import "fmt"

/* Added by Yang */
import (
    "sync"
	"log"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}


func (mr *MapReduce) HandOutJob(jobType JobType) {
    // Initialize
    var wg sync.WaitGroup
    var nNumCurPhase, nNumOtherPhase int
    if Map == jobType {
        nNumCurPhase = mr.nMap
        nNumOtherPhase = mr.nReduce
    } else {
        nNumCurPhase = mr.nReduce
        nNumOtherPhase = mr.nMap
    }
	cUndoJobChannel := make(chan int, nNumCurPhase)
	defer close(cUndoJobChannel)
	for i := 0; i < nNumCurPhase; i++ {
	    cUndoJobChannel <- i
	    wg.Add(1)
	}
	// Hand out job using go
	go func(cUndoJobChannel chan int) {
        for nJob, ok := <- cUndoJobChannel; ok != false; nJob, ok = <- cUndoJobChannel{
            WorkerAddress := <- mr.workerChannel
	        go func(cUndoJobChannel chan int, nJob int, WorkerAddress string){
        	    args := &DoJobArgs{mr.file, jobType, nJob, nNumOtherPhase}
	            var reply DoJobReply
	            ok := call(WorkerAddress, "Worker.DoJob", args, &reply)
	            
	            if ok == false {
		            fmt.Printf("RunMaster: RPC %s DoJob on %s error\n", WorkerAddress, jobType)
		            cUndoJobChannel <- nJob
		            delete(mr.Workers, WorkerAddress)
	            } else {
	                wg.Done()
	                mr.workerChannel <- WorkerAddress
	            }
            }(cUndoJobChannel, nJob, WorkerAddress)
	    }
    }(cUndoJobChannel)
    // Wait for completing
    wg.Wait()
}


// 1. The master should communicate with the workers via RPC.(go statement)
// 2. Hand out the map and reduce jobs to workers,
//    and return only when all the jobs have finished.
// 3. RunMaster only needs to tell the workers
//    the name of the original input file (mr.file) and the job number
// 4. Your RunMaster should process new worker registrations by
//    reading from mr.registerChannel
// 5. Modify the MapReduce struct to keep track of any additional state
//    and initialize this additional state in the InitMapReduce() function.
// 1. 传入goroutine参数完成变量线程安全
// 2. 利用waitgroup计数
// 3. channel是定长的以及其阻塞机制
// 4. 利用close停止channel阻塞
// 5. map线程安全问题（同一位置读写或结构性质）
// 6. 等待子线程完成后再退出，否则子线程会强制结束
// 7. defer
// 8. 限制goroutine生成数量
func (mr *MapReduce) RunMaster() *list.List {
    go func() {
        for true {
            WorkerAddress := <-mr.registerChannel
            mr.Workers[WorkerAddress] = &WorkerInfo{WorkerAddress}
            mr.workerChannel <- WorkerAddress
        }
    }()

    log.Printf("Hand out map job\n")
    mr.HandOutJob(Map)
    log.Printf("Hand out reduce job\n")
    mr.HandOutJob(Reduce)

    log.Printf("Kill workers and return\n")
    return mr.KillWorkers()
}
