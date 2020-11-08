package mapreduce

import "container/list"
import "fmt"
import "net/rpc"


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

func (mr *MapReduce) AssignJobsToworkers(job JobType, do_job_arg *DoJobArgs, worker_addr string, rpc_ok_chan chan bool, doneChannel chan bool){
	
	//교수님.. 아래 코드는 제가 짠 것이 아니라 한승규생도의 도움을 받아 코드를 작성했습니다..
	//사실 아직 코드를 잘 이해하지 못하겠습니다.. 백
	
	reply := new(DoJobReply)
	
	ok := call(worker_addr, "Worker.DoJob", do_job_arg, &reply)
	
	if ok == false {
		fmt.Printf(string(job)), "worker: %s error\n", worker_addr)
		rpc_ok_chan <- false
		return
		
	}else {
		rpc_ok_chan <- true
		doneChannel <- reply.OK
		fmt.Println(job, "done : ", do_job_arg.JobNumber)
	}
}
	
	return mr.KillWorkers()
}

(mr *MapReduce) RunMaster() *list.List{
	
	//교수님.. 전공생도들 도움을 받아도 코드를 이해하는 것이 너무 어려웠습니다.. 죄송합니다!
	
	return mr.KillWorkers()
}





