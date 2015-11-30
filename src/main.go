package main

import (
	"encoding/json"
	"flag"
	"github.com/weilaihui/fdfs_client"
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
	defaultTestDesc := `
{
	"TotalCount":1000,
	"Parallel":10,
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
	}
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
	fdfsCfgFile := flag.String("fa", "", "(must)fastDFS config file path")
	origUsage := flag.Usage
	flag.Usage = func() {
		log.Println("the default test description is")
		log.Println(defaultTestDesc)
		origUsage()
	}
	testDescStr := flag.String("td", "see default test description", "(opt)test description")
	flag.Parse()
	if *fdfsCfgFile == "" {
		flag.Usage()
		return
	}
	log.Println("to use fastDFS config file", *fdfsCfgFile)
	if *testDescStr == "see default test description" {
		log.Println("to use default test description")
		*testDescStr = defaultTestDesc
	}

	td := testDesc{}
	if err := json.Unmarshal([]byte(*testDescStr), &td); err != nil {
		log.Println(err)
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

	var err error

	writeBuf := make([]byte, maxBufLen)
	taskChan := make(chan task, td.Parallel * 2)
	readTimeChan := make(chan int64, td.Read.Count)
	writeTimeChan := make(chan int64, td.Write.Count)
	workerWg := sync.WaitGroup{}

	writeCount := int64(0)
	readCount := int64(0)

	var client *fdfs_client.FdfsClient
	if client,err=fdfs_client.NewFdfsClient("client.conf");err != nil{
		log.Fatal(err)
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
					 client.DeleteFile(i.id)
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
						 id := doWrite(client, writeBuf[0:t.writeLen])
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
							 inc := doRead(client, id.id)
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

	os.Stdout.Write([]byte(fmt.Sprintf("%v\n", readPts)))
	os.Stdout.Write([]byte(fmt.Sprintf("%v\n", writePts)))
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

func doRead(client *fdfs_client.FdfsClient, id string) int{
	if rsp,err:=client.DownloadToBuffer(id, 0, 0);err != nil || rsp.DownloadSize < 0{
		log.Fatalln("download ", id, " error:", err)
		return -1
	}else{
		return int(rsp.DownloadSize)
	}
}

func doWrite(client *fdfs_client.FdfsClient, data []byte) string{
	if rsp, err :=client.UploadByBuffer(data, ".raw");err != nil{
		log.Fatalln("upload error:", err)
		return ""
	}else {
		return rsp.RemoteFileId
	}
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