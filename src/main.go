package main

import (
	"encoding/json"
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"math/rand"
	"time"
	"net/http"
	//"github.com/jtolds/gls"
	"os"
	"os/signal"
	"runtime"
	"fmt"
	"fs"
	"io/ioutil"
)

type testDesc struct {
	TotalCount int
	Parallel int
	Write struct {
		Pct int
		Count int
		Cfg []writeCfg
		Clean bool
	}
	Read struct {
		Pct int
		Count int
	}
	AvgOpsInv int
}

type task struct{
	//readId string
	writeLen int
}
type writeCfg struct{
	Max int
	Min int
	Pct int
	Count int
}

type reportStr struct{
	Start time.Time
	End time.Time
	Duration time.Duration
	AvgWriteDuration time.Duration
	AvgReadDuration time.Duration
	TotalWriteBytes int64
	TotalReadBytes int64
	TotalWriteTime int64
	TotalReadTime int64
}

func main() {
	log.SetFlags(log.Llongfile)
	defaultTestDesc := `
{
	"TotalCount":100,
	"Parallel":1,
	"Write":{
		"Pct":90,
		"Cfg":[
			{"Max":15000, "Min":3000, "Pct":95},
			{"Max":300000, "Min":100000, "Pct":3},
			{"Max":800000, "Min":300000, "Pct":2}
		],
		"Clean":true
	},
	"Read":{
		"Pct":10
	},
	"AvgOpsInv":250
}
`
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(){
		for _ = range c {
			RecogStack()
		}
	}()

	var report reportStr
	fsType := flag.String("ft", "fastdfs", "(must) fastdfs or seaweedfs")
	fdfsCfgFile := flag.String("fa", "", "(must for fastdfs)fastDFS config file path")
	seaweedfsUrlRoot := flag.String("sr", "", "(must for seaweedfs)seaweedfs master server and port, like 127.0.0.1:9333")
	origUsage := flag.Usage
	flag.Usage = func() {
		log.Println("the default test description is")
		log.Println(defaultTestDesc)
		origUsage()
	}
	testDescStr := flag.String("td", "see default test description", "(opt)test description")
	flag.Parse()
	if *fdfsCfgFile == "" && *fsType=="fastdfs"{
		flag.Usage()
		return
	}
	if *seaweedfsUrlRoot=="" && *fsType=="seaweedfs"{
		flag.Usage()
		return
	}
	//log.Println("to use fastDFS config file", *fdfsCfgFile)
	if *testDescStr == "see default test description" {
		log.Println("to use default test description")
		*testDescStr = defaultTestDesc
	}

	td := testDesc{}
	if err := json.Unmarshal([]byte(*testDescStr), &td); err != nil {
		log.Println(err)
		synerr,ok:=err.(*json.SyntaxError)
		if ok{
			begin := synerr.Offset - 5
			if begin < 0{
				begin = 0
			}
			log.Println(synerr.Offset, (*testDescStr)[begin:synerr.Offset])
		}
		flag.Usage()
		return
	}
	td.Write.Count = td.TotalCount*td.Write.Pct / 100
	td.Read.Count = td.TotalCount*td.Read.Pct / 100
	maxBufLen := 0
	for _,c:= range td.Write.Cfg{
		if maxBufLen < c.Max{
			maxBufLen = c.Max
		}
		c.Count = c.Pct * td.Write.Count
	}

	writeBuf := make([]byte, maxBufLen)
	taskChan := make(chan task, td.Parallel * 2)
	readTimeChan := make(chan int64, td.Read.Count)
	writeTimeChan := make(chan int64, td.Write.Count)
	workerWg := sync.WaitGroup{}

	writeCount := int64(0)
	readCount := int64(0)

	var client fstestbenchmark.Fs
	switch(*fsType){
	case "fastdfs":
		client = fstestbenchmark.NewFdfsClient(*fdfsCfgFile)
	case "seaweedfs":
		client = fstestbenchmark.NewWeedFsClient("http://" + *seaweedfsUrlRoot)
	}


	for i:=0;i<td.Parallel;i++{
		workerWg.Add(1)
		 go func(i int){
			 //mgr:=gls.NewContextManager();
			 //mgr.SetValues(gls.Values{"id":i}, func() {
			 type writeRslt struct{
				 id string
				 size int
			 }
			 wroteIDS := make([]writeRslt, 0)
			 defer func() {
				 for _,i:=range wroteIDS{
					 client.DoDelete(i.id)
				 }
				 log.Println("to quit worker ", i)
				 workerWg.Done()
			 }()

			 idsLen := 0
			 rnd := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))

			 loop:
			 for {
				 //log.Println(i, " waitting task")
				 select {
				 //for t:= range taskChan{
				 case t, ok := <-taskChan:
					 if t.writeLen > 0 {
						 wc := int(atomic.AddInt64(&writeCount, 1))
						 rd := int(atomic.LoadInt64(&readCount))
						 start := time.Now()
						 //log.Println(i, "to write ", t.writeLen)
						 if td.AvgOpsInv > 0 {
							 <-time.Tick(time.Second * time.Duration(rnd.Int63n(int64(td.AvgOpsInv) * 2)))
						 }
						 id,err := client.DoWrite("filename", writeBuf[0:t.writeLen])
						 if err != nil {
							 log.Fatalln(err)
						 }
						 //log.Println(i, "done write ", t.writeLen)
						 wt:=int64(time.Now().Sub(start))
						 atomic.AddInt64(&report.TotalWriteTime, wt)
						 writeTimeChan <- wt
						 wroteIDS = append(wroteIDS, writeRslt{id,int(t.writeLen)})
						 idsLen++
						 if reachPct(int64(td.Write.Pct), int64(wc), int64(rd + wc)) {
							 atomic.AddInt64(&readCount, 1)
							 start := time.Now()
							 id := wroteIDS[rnd.Int31n(int32(idsLen))]
							 log.Println(i, "to read ", id.id)
							 if td.AvgOpsInv > 0 {
								 <-time.Tick(time.Second * time.Duration(rnd.Int63n(int64(td.AvgOpsInv) * 2)))
							 }
							 inc,_,err := client.DoRead(id.id)
							 if err != nil {
								 log.Fatalln(err)
							 }
							 //log.Println(i, "done read ", id.id)
							 if inc != id.size {
								 log.Fatalln("wrote ", id.size, " but read ", inc, " for file:", id.id)
							 }
							 rt:=int64(time.Now().Sub(start))
							 readTimeChan<- rt
							 atomic.AddInt64(&report.TotalReadTime, rt)
							 atomic.AddInt64(&report.TotalReadBytes, int64(inc))
						 }
					 }
					 if !ok {
						 break loop
					 }
				 }
			 }
		}(i)
	}

	writeFall := make([]*writeCfg, 100)
	for idx,i:=0,0;i<len(td.Write.Cfg);i++{
		for j:=0;j<td.Write.Cfg[i].Pct;j++ {
			writeFall[idx]=&td.Write.Cfg[i]
			idx++
		}
	}
	if writeFall[len(writeFall) - 1] == nil{
		log.Fatalln("write cfg pcts not 100%")
	}

	rnd := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	report.Start = time.Now()
	for i:=0;i<td.Write.Count;i++{
		c := writeFall[int(rnd.Int31n(100))]
		t := task{}
		t.writeLen = c.Min + int(rnd.Int31n(int32(c.Max - c.Min)))
		//log.Println("submit write task len:", t.writeLen)
		taskChan <- t
		report.TotalWriteBytes += int64(t.writeLen)
	}
	close(taskChan)

	ptsReaderWg :=sync.WaitGroup{}
	readPts := make([]int64,0,td.Read.Count)
	ptsReaderWg.Add(1)
	go func(){
		defer func(){
			ptsReaderWg.Done()
		}()
		for {
			rt,ok:= <- readTimeChan
			if ok {
				readPts=append(readPts, rt)
			}else{
				break;
			}
		}
	}();

	writePts := make([]int64,0,td.Write.Count)
	ptsReaderWg.Add(1)
	go func(){
		defer func(){
			ptsReaderWg.Done()
		}()
		for {
			wt,ok := <- writeTimeChan
			if ok {
				writePts=append(writePts, wt)
			}else{
				break;
			}
		}
	}()

	log.Println("to wait task finish")
	workerWg.Wait()
	report.End = time.Now()
	close(writeTimeChan)
	close(readTimeChan)
	ptsReaderWg.Wait()

	//os.Stdout.Write()
	//os.Stdout.Write([]byte(fmt.Sprintf("%v\n", writePts)))
	ioutil.WriteFile("readPts.txt", []byte(fmt.Sprintf("%v\n", readPts)), os.ModePerm)
	ioutil.WriteFile("writePts.txt", []byte(fmt.Sprintf("%v\n", writePts)), os.ModePerm)
	report.Duration = report.End.Sub(report.Start)
	report.AvgReadDuration = time.Duration(report.TotalReadTime / readCount)
	report.AvgWriteDuration = time.Duration(report.TotalWriteTime / writeCount)
	log.Println("total write:",writeCount, " total read:",readCount)
	if rsp,err:=json.MarshalIndent(report,"","\t");err==nil{
		log.Println(string(rsp))
		log.Println("visit :4901 for report")

		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Write(rsp)
		})

		//log.Fatal(http.ListenAndServe(":4901", nil))
	}else{
		log.Fatal("marshal err:", err)
	}
}

func reachPct(pct,cnt,total int64) bool{
	return cnt > (total * pct / 100)
}

func isexist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func outputStack(fname string) {
	buf := make([]byte, 1<<20)
	runtime.Stack(buf, true)

	fd, err := os.Create(fname)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
	}
	defer fd.Close()

	fd.Write(buf)           //write file
	fmt.Printf(string(buf)) //print stdout
}

func RecogStack() {
	count := 0
	cur_name := ""
	for ; ; count++ {
		cur_name = fmt.Sprintf("debug_stack.log.%d", count)
		if !isexist(cur_name) {
			break
		}
	}
	outputStack(cur_name)
}
