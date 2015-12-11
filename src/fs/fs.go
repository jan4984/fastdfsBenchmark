package fstestbenchmark

type Fs interface{
	DoRead(path string) (int,[]byte,error)
	DoWrite(refPath string, data[]byte, params string) (finalPath string, err error)
	DoDelete(path string) error
}
