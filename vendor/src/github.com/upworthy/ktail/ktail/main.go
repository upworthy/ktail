package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"github.com/upworthy/ktail"
	"io"
	"log"
	"os"
	"time"
)

var gzipMagic = []byte{0x1f, 0x8b}

func gzipDecode(b []byte) ([]byte, error) {
	if r, err := gzip.NewReader(bytes.NewBuffer(b)); err != nil {
		return nil, err
	} else {
		defer r.Close()
		ret := new(bytes.Buffer)
		if _, err := ret.ReadFrom(r); err != nil {
			return nil, err
		}
		return ret.Bytes(), nil
	}
}

func assertSpecified(f, message string) {
	if f == "" {
		log.Print(message)
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Lshortfile)

	region := flag.String("r", "", "AWS `region`")
	stream := flag.String("s", "", "AWS Kinesis `stream` to read from")
	iterType := flag.String("t", "LATEST", "AWS Kinesis stream iterator `type` that indicates where to start iteration")
	rest := flag.Duration("w", time.Second, "`duration` of time to rest between AWS Kinesis GetRecords calls")
	sep := flag.String("sep", "", "print a separator string after each AWS Kinesis record"+
		" (e.g. in Bash you might specifiy $'\\n' if you want add a new line after each record printed)")

	flag.Parse()
	assertSpecified(*stream, "You didn't specify a stream")
	assertSpecified(*iterType, "You didn't specify an iterator type")

	c := make(chan *ktail.Record)
	f := ktail.NewFollower(*region)
	rrc, err := f.Tail(c, *stream, *iterType, *rest)
	if err != nil {
		log.Fatal(err)
	}

	// watch for shard reading results
	ok := true
	go func() {
		for rr := range rrc {
			log.Printf("Reading routine terminated: %v", rr)
			ok = ok && rr.Err == io.EOF
		}
		close(c)
	}()

	for r := range c {
		data := r.Data
		if bytes.HasPrefix(data, gzipMagic) {
			var err error
			if data, err = gzipDecode(r.Data); err != nil {
				log.Print(err)
				continue
			}
		}
		if _, err := os.Stdout.Write(append(data, *sep...)); err != nil {
			log.Print(err)
		}
	}

	if !ok {
		os.Exit(1)
	}
}
