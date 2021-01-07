package mr


import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "strconv"
import "os"
import "errors"
import "time"
import "sort"

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue 

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	var mapping bool = true
	var finished bool = false
	//fmt.Println("worker start", os.Getpid())

	for {
		heartBeatReply := HeartBeat()
		mapping = !heartBeatReply.MapFinished
		finished = heartBeatReply.ReduceFinished

		//fmt.Println("got beat", os.Getpid(), mapping, finished)
		if mapping == true {
			//do map
			//fmt.Println("ready get map", os.Getpid())
			fileReply, err := GetWork()

			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}
			//fmt.Println("doing map", fileReply.FileName, os.Getpid())
			reduceNum := fileReply.ReduceNum


			file, err := os.Open(fileReply.FileName)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot open %v", fileReply.FileName)
			}

			content, err := ioutil.ReadAll(file)
			if err!= nil {
				log.Fatalf("cannot read %v", fileReply.FileName)
			}

			kva := mapf(fileReply.FileName, string(content))

			id := fileReply.MapID

			tmpName := make([]string, reduceNum + 1)
			tmpFile := make([]*os.File, reduceNum + 1)
			for i := 1; i <= reduceNum; i++ {
				tmpfile, err := ioutil.TempFile("", "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i))

				if err != nil {
					log.Fatal(err)
				}

				tmpName[i] = tmpfile.Name()
				tmpFile[i] = tmpfile
			}

			//map out to reduce bucket according to key
			for _, kv := range kva {
				reduceIndex := ihash(kv.Key) % reduceNum

				if reduceIndex == 0 {
					reduceIndex = reduceNum
				}

				oFile := tmpFile[reduceIndex]

				enc := json.NewEncoder(oFile)
				enc.Encode(&kv)

			}

			//rename tmp file
			for i := 1; i <= reduceNum; i++{
				os.Rename(tmpName[i], "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i))
			tmpName[i] = "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(i)
			}
			//communicate with master
			//about finishment and file location
			MapDone(fileReply.FileName)

			for i := 1; i <= reduceNum; i++ {
				MapOut(tmpName[i], i)
			}
			//fmt.Println("map finished", fileReply.FileName, os.Getpid())
		} else if finished == false{
			//do reduce
			filesReply := GetReduceWork()
			reduceNumber := filesReply.ReduceNumber
			files := filesReply.Files

			oname := "mr-out-" + strconv.Itoa(reduceNumber)
			ofile, _ := os.Create(oname)

			kva := make([]KeyValue, 0)

			for _, fileName := range files {
				file, _ := os.Open(fileName)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue

					if err := dec.Decode(&kv); err != nil {
						break
					}

					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}

				values := make([]string, 0)

				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}

				output := reducef(kva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j

				values = make([]string, 0)
			}
			//fmt.Println("reduce done", reduceNumber)
			ReduceDone(reduceNumber)
		} else if finished == true {
			//fmt.Println(" woker exit ", os.Getpid())
			break
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetWork() (*FileReply, error) {
	reply := FileReply{}
	if call("Master.GetWork", &ExampleArgs{}, &reply) == true{
		return &reply, nil
	} else{
		return &reply, errors.New("no idle work now")
	}
}

func GetReduceWork() *ReduceReply {
	reply := ReduceReply{}
	call("Master.GetReduceWork", &ExampleArgs{}, &reply)

	return &reply
}

func MapDone(fileName string) {
	call("Master.MapDone", &fileName, &ExampleReply{})
}

func MapOut(fileName string, reduceNum int) {

	call("Master.MapOut", &MapOutArg{FileName: fileName, ReduceNum: reduceNum}, &ExampleReply{})
}

func ReduceDone(reduceNum int) {
	call("Master.ReduceDone", &reduceNum, &ExampleReply{})
}

func HeartBeat() *HeartBeatReply {
	reply := HeartBeatReply{}
	call("Master.HeartBeat", &ExampleArgs{}, &reply)
	return &reply
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
