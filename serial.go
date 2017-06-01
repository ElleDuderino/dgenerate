package main

import (
	"fmt"
	"log"
	"strings"
)

func (s SerialFragment) Process() {

	if strings.HasPrefix(strings.ToUpper(s.Datatype), "INT") {
		s.processInt()
	} else if strings.HasPrefix(strings.ToUpper(s.Datatype), "FLOAT") {
		s.processFloat()
	} else {
		log.Fatalf("Can only generate numeric Serial distribution\n")
	}
}

func (s SerialFragment) processInt() {
	fd, f := s.GetOutputFile()
	defer fd.Close()

	start := int64(s.Sd.NumStart)
	for i := int64(s.RangeStart) + start; i <= start+int64(s.RangeEnd); i++ {
		_, err := f.WriteString(fmt.Sprintf("%v\n", i))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", s.Fragment)
		}
		f.Flush()
	}
}

func (s SerialFragment) processFloat() {
	fd, f := s.GetOutputFile()
	defer fd.Close()

	start := float64(s.Sd.NumStart)
	for i := float64(s.RangeStart) + start; i <= start+float64(s.RangeEnd); i++ {
		_, err := f.WriteString(fmt.Sprintf("%v\n", i))
		if err != nil {
			log.Fatalf("Error generating data for Fragment %v\n", s.Fragment)
		}
		f.Flush()
	}
}

func (s SerialFragment) Print() {
	fmt.Printf("%+v %+v\n", s.Fragment, s.Sd)
}
