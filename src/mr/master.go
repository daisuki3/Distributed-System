package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "errors"

type Master struct {
	// Your definitions here.
	idle Files
	record map[string]int
	reduceLocation map[int][]string

	reduceNum int
	totalNum int
	finishedNum int

	mapping bool
	mx sync.Mutex
}


type Files struct {
	files []string
	mutex sync.Mutex
}


const Unstarted int = 0
const MapFinished int = 1
const Processing int = 3
var mapID int  = 1
var reduceID int = 1
// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetWork(fake *ExampleArgs,reply *FileReply) error {
	//fmt.Println("server getting map ", os.Getpid())
	m.idle.mutex.Lock()
	
	
	length := len(m.idle.files)
	
	//fmt.Println("now length map ", length)

	if length == 0 {
		m.idle.mutex.Unlock()
		return errors.New("no idle work")
	}
	

	reply.FileName = m.idle.files[length - 1]
	reply.ReduceNum = m.reduceNum
	reply.MapID = mapID
	mapID = mapID + 1


	m.idle.files = m.idle.files[:length - 1]
	m.record[reply.FileName] = Processing

	//not finish in 10s,assume worker is dead
	//give it back to ilde


	m.idle.mutex.Unlock()

	go func (fileName string) {
		time.Sleep(10 * time.Second)

		m.mx.Lock()
		if m.record[fileName] == Processing {
			//fmt.Println("re-schedule, work ", fileName)
			
			//fmt.Println(len(m.idle.files))
			m.idle.files = append(m.idle.files, fileName)
			m.record[fileName] = Unstarted
			//fmt.Println("after re-schedule ", len(m.idle.files))
		}
		m.mx.Unlock()

	}(reply.FileName)

	return nil
}

func (m *Master) GetReduceWork(fake *ExampleArgs, reply *ReduceReply) error{
	m.mx.Lock()
	defer m.mx.Unlock()
	reduceNumber := reduceID % m.reduceNum
	if reduceNumber == 0 {
		reduceNumber = m.reduceNum
	}
	reduceID = reduceID + 1
	reply.Files = m.reduceLocation[reduceNumber][:]
	reply.ReduceNumber = reduceNumber
	return nil
}

func (m *Master) HeartBeat(fake *ExampleArgs, reply *HeartBeatReply) error{
	if m.mapping == true {
		if m.finishedNum == m.totalNum {
			m.mapping = false
			reply.MapFinished = true
			m.finishedNum = 0
		} else{
			reply.MapFinished = false
		}
		reply.ReduceFinished = false
	} else {
		reply.MapFinished = true
		if m.finishedNum >= m.reduceNum {
			reply.ReduceFinished = true
		} else {
			reply.ReduceFinished = false
		}
	}
	return nil
}

func (m *Master) MapDone (fileName *string, fake *ExampleReply) error {
	m.mx.Lock()
	if m.record[*fileName] == Processing {
		m.record[*fileName] = MapFinished
		m.finishedNum = m.finishedNum + 1
		m.mx.Unlock()
		return nil
	}
	m.mx.Unlock()
	return errors.New("failed,because you were timeout")
}

func (m *Master) MapOut (arg *MapOutArg, fake *ExampleReply) error {
	reduceNum := arg.ReduceNum
	fileName := arg.FileName
	m.mx.Lock()
	m.reduceLocation[reduceNum] = append(m.reduceLocation[reduceNum], fileName)
	m.mx.Unlock()
	return nil
}


func (m *Master) ReduceDone (reduceNum *int, fake *ExampleReply) error {
		m.mx.Lock()
		m.finishedNum = m.finishedNum + 1
		m.mx.Unlock()
		return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	if m.finishedNum == m.reduceNum && m.mapping == false {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}


	// Your code here.
	m.idle.files = files[:]
	m.record = make(map[string]int)

	for _, fileName := range files {
		//m.all.files[i].fileName = fileName
		//m.all.files[i].status = Unstarted
		m.record[fileName] = Unstarted
	}

	m.reduceLocation = make(map[int][]string)
	m.reduceNum = nReduce

	m.totalNum = len(files)
	m.finishedNum = 0

	m.mapping = true

	m.server()
	return &m
}
