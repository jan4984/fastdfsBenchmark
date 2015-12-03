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
)

type weedclient struct{
	root string
	volMapLock sync.RWMutex
	volMap map[string]string
}

type volLookUpRst struct{
	locations []struct{
		publicUrl string `json:"publicUrl"`
	} `json:"locations"`
}

func (this *weedclient)fetchVolumeUrl(id string) (string, error){
	rsp, err := http.Get(this.root + "/dir/lookup?volumeId="+id)
	if err != nil {
		return "", err
	}
	jd:=json.NewDecoder(rsp.Body)
	defer rsp.Body.Close()

	var rst volLookUpRst
	if err := jd.Decode(&rst);err != nil{
		return "", err
	}

	if len(rst.locations) > 0{
		return rst.locations[0].publicUrl
	}

	return "", errors.New("no url for id "+id)
}

func (this* weedclient)fetchFile(path string) ([]byte, error){
	rsp, err := http.Get(path)
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
	return &weedclient{urlroot};
}

func (this *weedclient)DoRead(path string) (int, []byte, error){
	var err error
	var vurl string
	var ok bool

	vid := getVolId(path)
	if vid == ""{
		return -1,nil,errors.New("wrong path:"+path)
	}

	this.volMapLock.RLock()
	if vurl,ok = this.volMap[vid];!ok{
		this.volMapLock.RUnlock()

		vurl, err = this.fetchVolumeUrl(vid)
		if(err != nil){
			return -1,nil,err
		}
		this.volMapLock.Lock()
		this.volMap[vid]=vurl
		this.volMapLock.Unlock()
	}else {
		this.volMapLock.RUnlock()
	}
	var buf []byte
	buf, err = this.fetchFile(vurl+"/"+path);
	if err != nil{
		return -1,nil,err
	}
	return len(buf),buf,nil
}
type assignRst struct{
	fid string `json:"fid"`
}
type wroteRst struct{
	name string `json:"name"`
	size int `json:"size"`
}
func (this *weedclient)DoWrite(refPath string, data[]byte) (string,error){
	rsp,err := http.Post(this.root+"/dir/assign", "text/plain", nil)
	if err != nil {
		return "", err
	}
	jd:=json.NewDecoder(rsp.Body)
	defer rsp.Body.Close()

	var fid string
	{
		rst := assignRst{}
		if err := jd.Decode(&rst); err != nil {
			return "", err
		}
		fid = rst.fid
	}

	body := &bytes.Buffer{}
	instream := bytes.NewBuffer(data)
	multipartWriter := multipart.NewWriter(body)
	wr, err := multipartWriter.CreateFormFile("file", refPath)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(wr, instream)
	if err != nil && err != io.EOF {
		return "", err
	}
	multipartWriter.Close();

	req,err := http.NewRequest("PUT", this.root + "/" + fid, body)
	if err != nil{
		return "", err
	}

	rsp, err = http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	jd = json.Decoder{rsp.Body}
	defer rsp.Body.Close()

	rst := wroteRst{}
	if err := jd.Decode(&rst);err != nil{
		return "",err
	}

	if rst.size != len(data){
		return "", errors.New("wrote " + rst.size + " but required " + len(data))
	}

	return fid, nil

}
func (this *weedclient)DoDelete(path string) error{
	req,err := http.NewRequest("DELETE", this.root + "/" + path, nil)
	if err != nil {
		return err
	}
	rsp,err :=http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != 200 {
		return errors.New("response status " + rsp.StatusCode)
	}
	return nil
}

