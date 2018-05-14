package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/itchio/wharf/wrand"

	"github.com/itchio/wharf/eos/option"

	"github.com/itchio/wharf/eos"
)

type fakeFileSystem struct {
	fakeData []byte
}

func (ffs *fakeFileSystem) Open(name string) (http.File, error) {
	br := bytes.NewReader(ffs.fakeData)
	ff := &fakeFile{
		Reader: br,
		FS:     ffs,
	}
	return ff, nil
}

type fakeFile struct {
	*bytes.Reader
	FS *fakeFileSystem
}

func (ff *fakeFile) Stat() (os.FileInfo, error) {
	return &fakeStats{fakeFile: ff}, nil
}

func (ff *fakeFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, nil
}

func (ff *fakeFile) Close() error {
	return nil
}

type fakeStats struct {
	fakeFile *fakeFile
}

func (fs *fakeStats) Name() string {
	return "bin.dat"
}

func (fs *fakeStats) IsDir() bool {
	return false
}

func (fs *fakeStats) Size() int64 {
	return int64(len(fs.fakeFile.FS.fakeData))
}

func (fs *fakeStats) Mode() os.FileMode {
	return 0644
}

func (fs *fakeStats) ModTime() time.Time {
	return time.Now()
}

func (fs *fakeStats) Sys() interface{} {
	return nil
}

func main() {
	log.Printf("Generating fake data...")
	prng := &wrand.RandReader{
		Source: rand.NewSource(time.Now().UnixNano()),
	}
	fakeData, err := ioutil.ReadAll(io.LimitReader(prng, 4*1024*1024))
	must(err)

	http.Handle("/", http.FileServer(&fakeFileSystem{fakeData}))

	log.Printf("Starting http server...")
	l, err := net.Listen("tcp", "localhost:0")
	must(err)

	go func() {
		log.Fatal(http.Serve(l, nil))
	}()

	url := fmt.Sprintf("http://%s/file.dat", l.Addr().String())

	f, err := eos.Open(url, option.WithHTFSCheck())
	must(err)

	done := make(chan bool)

	worker := func(workerNum int) {
		source := rand.NewSource(time.Now().UnixNano())
		buf := make([]byte, 1024)

		for i := 0; i < 100*100; i++ {
			if i%100 == 0 {
				log.Printf("[%d] %d reads...", workerNum, i)
			}

			_, err := f.Read(buf)
			must(err)

			_, err = f.Seek((source.Int63()%int64(len(fakeData)))-int64(len(buf)), io.SeekStart)
			must(err)
		}
		done <- true
	}

	numWorkers := 1
	for i := 0; i < numWorkers; i++ {
		go worker(i)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}
}

func must(err error) {
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
}
