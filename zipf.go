package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

func (z ZipfFragment) Process() {

	if strings.HasPrefix(strings.ToUpper(z.Datatype), "INT") {
		z.processInt()
	} else {
		log.Fatalf("Can only generate Uint64 Zipfian distribution\n")
	}
}

func (z ZipfFragment) processInt() {
	fd, f := z.GetOutputFile()
	defer fd.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(r, z.Zd.S, z.Zd.V, z.Zd.Imax)
	var val uint64

	for i := 0; i < z.NumRows; i++ {
		val = zipf.Uint64()
		_, err := f.WriteString(fmt.Sprintf("%v\n", val))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", z.Fragment)
		}
		f.Flush()
	}
}

func (z ZipfFragment) Print() {
	fmt.Printf("%+v %+v\n", z.Fragment, z.Zd)
}
