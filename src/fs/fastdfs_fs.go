package fstestbenchmark

import(
	"github.com/weilaihui/fdfs_client"
	"log"
)

type fdfsclient struct{
	fdfs *fdfs_client.FdfsClient
}

func NewFdfsClient(cfgPath string ) Fs{
	var err error
	var c fdfsclient
	if c.fdfs,err=fdfs_client.NewFdfsClient("client.conf");err != nil{
		log.Fatal(err)
	}
	return &c;
}

func (this *fdfsclient)DoRead(path string) (int,[]byte,error){
	if rsp,err:=this.fdfs.DownloadToBuffer(path, 0, 0);err != nil || rsp.DownloadSize < 0{
		log.Fatalln("download ", path, " error:", err)
		return -1, nil, err;
	}else{
		return int(rsp.DownloadSize),rsp.Content.([]byte),nil
	}
}
func (this *fdfsclient)DoWrite(refPath string, data[]byte, params string) (string, error){
	if rsp, err :=this.fdfs.UploadByBuffer(data, ".raw");err != nil{
		log.Fatalln("upload error:", err)
		return "",err
	}else {
		return rsp.RemoteFileId,nil
	}
}
func (this *fdfsclient)DoDelete(path string) error{
	err := this.fdfs.DeleteFile(path)
	if err!=nil{
		log.Fatal(err)
		return err
	}
	return nil
}
/*
func (this *fdfsclient)isExist(path string) (bool,error){
	_,err:=this.fdfs.DownloadToBuffer(path, 0, 0)
	if err != nil {
		return false,err
	}
	return true, nil
}
*/