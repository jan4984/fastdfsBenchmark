package fstestbenchmark
import (
	"sync"
	"strings"
	"net/http"
	"encoding/json"
	"errors"
	"io/ioutil"
	"bytes"
	"mime/multipart"
	"io"
	"fmt"
	"log"
	"net"
	"time"
)

type weedclient struct{
	root string
	volMapLock *sync.RWMutex
	volMap map[string]string
	hc *http.Client
}

type volLookUpRst struct{
	Locations []struct{
		PublicUrl string `json:"publicUrl"`
	} `json:"locations"`
}

func (this *weedclient)fetchVolumeUrl(id string) (string, error){
	rsp, err := this.get(this.root + "/dir/lookup?volumeId="+id)
	if err != nil {
		return "", err
	}
	jd:=json.NewDecoder(rsp.Body)
	defer rsp.Body.Close()

	var rst volLookUpRst
	if err := jd.Decode(&rst);err != nil{
		return "", err
	}

	if len(rst.Locations) > 0{
		return rst.Locations[0].PublicUrl, nil
	}

	return "", errors.New("no url for id "+id)
}

func (this* weedclient)fetchFile(path string) ([]byte, error){
	rsp, err := this.get(path)
	if err != nil{
		return nil,err
	}
	b,err:=ioutil.ReadAll(rsp.Body)
	defer rsp.Body.Close()

	if err!=nil{
		return nil,err
	}
	return b,nil
}

func getVolId(path string) string{
	ss := strings.Split(path, ",")
	if len(ss) == 2 {
		return ss[0]
	}
	return ""
}

func NewWeedFsClient(urlroot string ) Fs{
	log.Println("new seaweedfs client to", urlroot)
	hc := http.Client{
		Transport:&http.Transport{
			Dial:(&net.Dialer{
				Timeout:5 * time.Second,
				KeepAlive:30 * time.Second,
			}).Dial,
			MaxIdleConnsPerHost:100,
		},
	}
	return &weedclient{urlroot, &sync.RWMutex{}, make(map[string]string), &hc};
}

func (this* weedclient)getVolumeUrl(path string)(string,error){
	vid := getVolId(path)
	this.volMapLock.RLock()
	if vurl,ok := this.volMap[vid];!ok{
		this.volMapLock.RUnlock()

		vurl, err := this.fetchVolumeUrl(vid)
		if(err != nil){
			return "",err
		}
		this.volMapLock.Lock()
		this.volMap[vid]=vurl
		this.volMapLock.Unlock()
		return vurl,nil
	}else {
		this.volMapLock.RUnlock()
		return vurl,nil
	}
}

func (this *weedclient)DoRead(path string) (int, []byte, error){
	vurl,err:=this.getVolumeUrl(path)
	if err!=nil{
		return -1,nil,err
	}

	var buf []byte
	buf, err = this.fetchFile("http://"+vurl+"/"+path);
	if err != nil{
		return -1,nil,err
	}
	return len(buf),buf,nil
}
type assignRst struct{
	Fid string `json:"fid"`
}
type wroteRst struct{
	Name string `json:"name"`
	Size int `json:"size"`
}
func (this *weedclient)post(url string, content_type string, body io.Reader) (rsp *http.Response, err error){
	var req *http.Request
	req,err = http.NewRequest("POST", url, body)
	if err != nil {
		return
	}
	rsp,err =this.hc.Do(req)
	return
}

func (this *weedclient)get(url string) (rsp *http.Response, err error){
	var req *http.Request
	req,err = http.NewRequest("GET", url, nil)
	if err != nil {
		return
	}
	rsp,err =this.hc.Do(req)
	return
}

func (this *weedclient)DoWrite(refPath string, data[]byte) (string,error){
	rsp,err := this.post(this.root+"/dir/assign", "text/plain", nil)
	if err == io.EOF{
		//for long-term running, i found that the assign api may EOF sometimes, simply try again
		log.Println("assign unexpected EOF, to try again now")
		rsp,err = this.post(this.root+"/dir/assign", "text/plain", nil)
		if err == nil {
			log.Println("retry successed.")
		}else{
			log.Println("retry failed:", err);
		}
	}
	if err != nil {
		return "", err
	}
	buf,err:=ioutil.ReadAll(rsp.Body)
	if err != nil {
		return "",err
	}
	//jd:=json.NewDecoder(rsp.Body)
	defer rsp.Body.Close()

	var fid string
	if fid,err = func() (string,error){
		rst := assignRst{}
		//log.Println("to unmarshal", string(buf))
		if err := json.Unmarshal(buf, &rst); err != nil {
			return "", errors.New(fmt.Sprintf("decoding assign result %s failed %v", string(buf), err))
		}
		if rst.Fid=="" {
			return "", errors.New(fmt.Sprintf("assign failed:%v", string(buf)))
		}
		fid = rst.Fid
		return fid,nil
	}();err != nil{
		return "",err
	}

	vurl,err := this.getVolumeUrl(fid)
	if err != nil{
		return "", err
	}

	body := &bytes.Buffer{}
	instream := bytes.NewBuffer(data)
	multipartWriter := multipart.NewWriter(body)
	wr, err := multipartWriter.CreateFormFile("file", refPath)
	if err != nil {
		return "", err
	}
	n, err := io.Copy(wr, instream)
	if err != nil && err != io.EOF {
		return "", err
	}
	if int(n) != len(data){
		return "", errors.New("not copy enough data")
	}
	multipartWriter.Close();

	req,err := http.NewRequest("PUT", "http://"+vurl + "/" + fid, body)
	if err != nil{
		return "", err
	}
	req.Header.Set("Content-Type", multipartWriter.FormDataContentType())

	rsp, err = this.hc.Do(req)
	if err != nil {
		return "", err
	}
	buf,err=ioutil.ReadAll(rsp.Body)
	if err == io.EOF{
		//for long-term running, i found that the assign api may EOF sometimes, simply try again
		log.Println("put", "http://"+vurl + "/" + fid,"unexpected EOF, to try again now")
		rsp, err = this.hc.Do(req)
		if err == nil {
			log.Println("retry successed.")
		}else{
			log.Println("retry failed:", err);
		}
	}
	if err != nil {
		return "",err
	}
	//jd := json.NewDecoder(rsp.Body)
	defer rsp.Body.Close()

	rst := wroteRst{}
	//log.Println("upload response:", string(buf))
	if err := json.Unmarshal(buf,&rst);err != nil{
		return "",errors.New(fmt.Sprintf("decoding assign result %s failed %v", string(buf), err))
	}

	if rst.Size != len(data){
		return "", errors.New(fmt.Sprintf("wrote %d but required %d", rst.Size, len(data)))
	}

	return fid, nil

}
func (this *weedclient)DoDelete(path string) error{
	req,err := http.NewRequest("DELETE", this.root + "/" + path, nil)
	if err != nil {
		return err
	}
	rsp,err :=this.hc.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != 200 {
		return errors.New(fmt.Sprint("response status " ,rsp.StatusCode))
	}
	return nil
}
